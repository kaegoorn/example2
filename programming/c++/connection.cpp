#include "connection.h"
#include "core/crypto/utils.h"
#include "core/microservice/eventloop.h"
#include "core/microservice/logger.h"
#include "core/network/dnsresolver.h"
#include "core/network/ssl/utils.h"
#include "recordset.h"
#include <libpq-fe.h>
#include <openssl/x509.h>
#include <unistd.h>
#include <uv.h>

  namespace core {
    namespace postgresql {

      Error Connection::SslTmpFile::create(std::string_view data) {
        f_ = std::tmpfile();
        if(!f_) {
          return MAKE_ERROR("Unable to create temporary ssl file. %s", strerror(errno));
        }
        std::fwrite(data.data(), data.size(), 1, f_);
        std::fflush(f_);
        path_ = "/proc/self/fd/" + std::to_string(fileno(f_));
        return Error::Success;
      }
      void Connection::SslTmpFile::clear() {
        if(f_) {
          fclose(f_);
          f_ = nullptr;
        }
        path_.clear();
      }

      Connection::SslTmpFile::~SslTmpFile() {
        if(f_) {
          fclose(f_);
        }
      }

      const std::string &Connection::SslTmpFile::path() const {
        return path_;
      }

      void Connection::SslTemporaryFiles::clear() {
        certificate_.clear();
        key_.clear();
        ca_.clear();
      }

      class Connection::ConnectionImpl {
      public:
        enum class State {
          Disconnected,
          Connecting,
          Connected,
          Disconnecting
        };

      public:
        ConnectionImpl(AsyncObjectPtr<Connection> base) :
            base_(base),
            requestId_(InvalidRequestId),
            connectTimer_(ConstructTag(STRING_VIEW("core::postgresql::Connection::ConnectionImpl::startTimer")), base->eventLoop()) {
          state_ = State::Connecting;
          connectTimer_->restart(base_->options().connectTimeout(), [this]() {
            reconnect(MAKE_ERROR("Connection timeout"));
          });
          network::DnsResolver::instance()->resolve(
              base_->options().hosts()[base_->hostIndex_],
              [this](const std::vector<core::network::Address> &addresses) {
                dnsRequestId_ = {};
                if(!base_) {
                  return;
                }
                if(addresses.empty()) {
                  reconnect(MAKE_ERROR("Unable to resolve host address \"%s\"", base_->options().hosts()[base_->hostIndex_].c_str()));
                  return;
                }
                connect(addresses[0].toString());
                return;
              },
              &dnsRequestId_);
        }

        ~ConnectionImpl() {
          disconnect();
        }

        void connect(std::string_view address) {
          std::string port = std::to_string(base_->options().port());
          std::string connect_timeout = std::to_string(std::chrono::duration_cast<std::chrono::seconds>(base_->options().connectTimeout()).count());
          std::vector<const char *> keywords;
          std::vector<const char *> values;
          if(!base_->options().databaseName().empty()) {
            keywords.push_back("dbname");
            values.push_back(base_->options().databaseName().c_str());
          }

          keywords.push_back("hostaddr");
          values.push_back(address.data());
          keywords.push_back("port");
          values.push_back(port.c_str());
          keywords.push_back("user");
          values.push_back(base_->options().userName().c_str());
          if(base_->options_.sslOptions().isAllow()) {
            keywords.push_back("host");
            values.push_back(base_->options().hosts()[base_->hostIndex_].c_str());
            keywords.push_back("sslmode");
            if(!base_->sslTemporaryFiles_.ca_.path().empty()) {
              values.push_back("verify-full");
              keywords.push_back("sslrootcert");
              values.push_back(base_->sslTemporaryFiles_.ca_.path().c_str());
            } else {
              values.push_back("require");
            }
            keywords.push_back("sslcert");
            values.push_back(base_->sslTemporaryFiles_.certificate_.path().c_str());
            keywords.push_back("sslkey");
            values.push_back(base_->sslTemporaryFiles_.key_.path().c_str());

          } else {
            keywords.push_back("password");
            values.push_back(base_->options().password().c_str());
          }
          keywords.push_back("connect_timeout");
          values.push_back(connect_timeout.c_str());

          keywords.push_back(nullptr);
          values.push_back(nullptr);
          handle_ = PQconnectStartParams(keywords.data(), values.data(), base_->options().databaseName().empty() ? 0 : 1);
          if(handle_ == nullptr) {
            reconnect(MAKE_ERROR("Connection to database failed."));
            return;
          }
          ConnStatusType status = PQstatus(handle_);
          if(status != CONNECTION_STARTED) {
            reconnect(MAKE_ERROR("Connection to database failed. %s", PQerrorMessage(handle_)));
            return;
          }

          PQsetNoticeReceiver(
              handle_,
              [](void *, const PGresult *) {
              },
              nullptr);
          PQsetNoticeProcessor(
              handle_,
              [](void *, const char *) {
              },
              nullptr);

          int fd = PQsocket(handle_);
          if(fd < 0) {
            reconnect(MAKE_ERROR("Unable to get socket description"));
            return;
          }

          fd_ = fcntl(fd, F_DUPFD_CLOEXEC, 0);
          if(fd_ < 0) {
            reconnect(MAKE_ERROR("Unable to duplicate socket description"));
            return;
          }
          base_->options().socketOptions().apply(fd);
          pollHandle_ = new uv_poll_t;
          uv_poll_init(base_->eventLoop()->handle(), pollHandle_, fd_);
          uv_handle_set_data(reinterpret_cast<uv_handle_t *>(pollHandle_), this);
          Error error = pollConnection();
          if(error.isFail()) {
            reconnect(error);
            return;
          }
        }

        void disconnect() {
          state_ = State::Disconnecting;

          if(base_) {
            base_->connectionImpl_ = nullptr;
            base_.reset();
            if(dnsRequestId_) {
              network::DnsResolver::instance()->cancelResolve(dnsRequestId_, false);
              dnsRequestId_ = {};
            }
            connectTimer_->stop();
            if(handle_ != nullptr) {
              if(fd_ >= 0) {
                close(fd_);
                fd_ = -1;
              }
              PQfinish(handle_);
              handle_ = nullptr;
              if(pollHandle_) {
                uv_poll_stop(pollHandle_);
                uv_close(reinterpret_cast<uv_handle_t *>(pollHandle_), [](uv_handle_t *poll) {
                  ConnectionImpl *_this = reinterpret_cast<ConnectionImpl *>(uv_handle_get_data(poll));
                  delete poll;
                  delete _this;
                });
              } else {
                delete this;
              }
            } else {
              delete this;
            }
          }
        }

        void reconnect(const Error &error) {
          AsyncObjectPtr<Connection> base = base_;
          disconnect();
          if(base) {
            if(base->options().isAutoReconnect()) {
              base->startReconnectTimer();
            }
            if(base->disconnectedHandler_) {
              base->disconnectedHandler_(error);
            }
          }
        }

        void finishRequest(const Error &error, Recordset &&result) {
          ExecuteHandler executeHandler = std::move(executeHandler_);
          isExecuting_ = false;
          executeHandler_ = {};
          requestId_ = InvalidRequestId;
          if(executeHandler) {
            if(error.isFail()) {
              executeHandler(error, std::move(result), base_);
              return;
            } else {
              executeHandler(Error::Success, std::move(result), base_);
              return;
            }
          }
        }

        static void pollConnectionCallback(uv_poll_t *handle, int status, int events) {
          ConnectionImpl *_this = reinterpret_cast<ConnectionImpl *>(uv_handle_get_data(reinterpret_cast<uv_handle_t *>(handle)));
          if(_this == nullptr) {
            return;
          }
          if(status < 0) {
            if(status == -9) {
              _this->reconnect(MAKE_ERROR("Unable to connect to postgresql server. %s", PQerrorMessage(_this->handle_)));
            } else {
              _this->reconnect(MAKE_ERROR("Bad status %d", status));
            }
            return;
          }
          if((events & ~(UV_READABLE | UV_WRITABLE)) != 0) {
            _this->reconnect(MAKE_ERROR("Unexpected event %d", events));
            return;
          }

          Error error = _this->pollConnection();
          if(error.isFail()) {
            _this->reconnect(error);
            return;
          }
        }

        static void pollCommandsCallback(uv_poll_t *handle, int status, int events) {
          ConnectionImpl *_this = reinterpret_cast<ConnectionImpl *>(uv_handle_get_data(reinterpret_cast<uv_handle_t *>(handle)));
          if(_this == nullptr) {
            return;
          }
          if(status < 0) {
            _this->reconnect(MAKE_ERROR("Bad status %d", status));
            return;
          }

          Error error = _this->pollCommands(events);
          if(error.isFail()) {
            _this->reconnect(error);
          }
        }

        Error pollCommands(int events) {
          int eventmask = eventmask_;

          if(events & UV_WRITABLE) {
            int rc = PQflush(handle_);
            if(rc == 0) {
              if(0) { // maybe_send_req()) {
                rc = PQflush(handle_);
                if(rc == 0) {
                  eventmask &= ~UV_WRITABLE;
                }
              } else {
                eventmask &= ~UV_WRITABLE;
              }
            }

            if(rc == 1) {
              eventmask |= UV_READABLE | UV_WRITABLE;
            } else if(rc != 0 && rc == -1) {
              return MAKE_ERROR("Unable to flush data to server. %s", PQerrorMessage(handle_));
            }
          }

          if(events & UV_READABLE) {
            if(!PQconsumeInput(handle_)) {
              return MAKE_ERROR("Unable to receive data from server. %s", PQerrorMessage(handle_));
            }

            if(!PQisBusy(handle_)) {
              Recordset result(PQgetResult(handle_));

              ExecStatusType res = PQresultStatus(result.handle());
              if(res != PGRES_TUPLES_OK && res != PGRES_COMMAND_OK) {
                while(PQgetResult(handle_)) {}
                finishRequest(MAKE_ERROR("Unable to execute %s", PQerrorMessage(handle_)), std::move(result));
                return Error::Success;
              }
              // TODO: handle more results
              if(PQgetResult(handle_) != nullptr) {
                return MAKE_ERROR("handling of more results is not supported");
              }

              finishRequest(Error::Success, std::move(result));

            } else {
              /* noop */
            }
          }

          return updatePollEventmask(eventmask);
        }

        Error pollConnection() {
          PostgresPollingStatusType rc;
          switch(state_) {
            case State::Connecting:
              rc = PQconnectPoll(handle_);
              break;
            case State::Disconnecting:
              rc = PQresetPoll(handle_);
              break;
            default:
              return MAKE_ERROR("Invalid state");
          }

          int events;
          uv_poll_cb cb;

          bool callConnectedHandler = false;
          switch(rc) {
            case PGRES_POLLING_READING: {
              events = UV_READABLE;
              cb = pollConnectionCallback;
              break;
            }
            case PGRES_POLLING_WRITING: {
              events = UV_WRITABLE;
              cb = pollConnectionCallback;
              break;
            }
            case PGRES_POLLING_OK: {
              connectTimer_->stop();
              state_ = State::Connected;
              eventmask_ = events = UV_WRITABLE | UV_READABLE;
              if(base_) {
                callConnectedHandler = true;
              }
              cb = pollCommandsCallback;
              break;
            }
            case PGRES_POLLING_FAILED: {
              return MAKE_ERROR("Polling failed. %s", PQerrorMessage(handle_));
            }
            default:
              return MAKE_ERROR("Unknown poll status");
          }

          int uvrc;
          if((uvrc = uv_poll_start(pollHandle_, events, cb)) != 0) {
            return MAKE_ERROR("Unable to start poll. %s\n", uv_strerror(uvrc));
          }
          if(PQstatus(handle_) == CONNECTION_OK) {
            if(callConnectedHandler) {
              if(base_->connectedHandler_) {
                Error error = base_->connectedHandler_();
                if(error.isFail()) {
                  return error;
                }
              }
            }
          }
          return Error::Success;
        }

        State state() const {
          return state_;
        }

        Error updatePollEventmask(int eventmask) {
          if(eventmask != eventmask_) {
            int rc = uv_poll_start(pollHandle_, eventmask, pollCommandsCallback);
            if(rc != 0) {
              return MAKE_ERROR("Unable to start poll. %s\n", uv_strerror(rc));
            }
            eventmask_ = eventmask;
          }
          return Error::Success;
        }

        PGconn *handle() const {
          return handle_;
        }

        bool isBusy() const {
          return isExecuting_;
        }

        Error execute(const char *query, const QueryData *queryData, Recordset *resultPtr) {
          if(state_ != ConnectionImpl::State::Connected) {
            return MAKE_ERROR("Connection is currently disconnected");
          }
          if(isBusy()) {
            return MAKE_ERROR("Connection is busy");
          }
          PGresult *r;
          if(queryData == nullptr) {
            r = PQexecParams(handle_, query, 0, nullptr, nullptr, nullptr, nullptr, 1);
          } else {
            r = PQexecParams(
                handle_, query, queryData->values().size(), queryData->types().data(), queryData->values().data(), queryData->lengths().data(), queryData->formats().data(), 1);
          }
          if(r == nullptr) {
            return MAKE_ERROR("Unable to create query");
          }
          Recordset result(r);
          ExecStatusType status = PQresultStatus(r);
          switch(status) {
            case PGRES_EMPTY_QUERY:
            case PGRES_COMMAND_OK:
              return Error::Success;
            case PGRES_TUPLES_OK:
              if(resultPtr) {
                resultPtr->~Recordset();
                new(resultPtr) Recordset(std::move(result));
              }
              return Error::Success;

            case PGRES_NONFATAL_ERROR:
            case PGRES_BAD_RESPONSE:
            case PGRES_FATAL_ERROR: {
              return MAKE_ERROR("Unable to execute query. %s", PQresultErrorMessage(result.handle()));
            }
            case PGRES_COPY_OUT:
            case PGRES_COPY_IN:
            case PGRES_COPY_BOTH:
            case PGRES_SINGLE_TUPLE:
            case PGRES_PIPELINE_SYNC:
            case PGRES_PIPELINE_ABORTED:
              return MAKE_ERROR("Unsupported query");
          }

          return Error::Success;
        }

        Error prepare(const char *name, const char *query, const std::vector<unsigned int> *types) {
          if(state_ != ConnectionImpl::State::Connected) {
            return MAKE_ERROR("Connection is currently disconnected");
          }
          if(isBusy()) {
            return MAKE_ERROR("Connection is busy");
          }
          PGresult *r;
          if(types) {
            r = PQprepare(handle_, name, query, types->size(), types->data());
          } else {
            r = PQprepare(handle_, name, query, 0, nullptr);
          }
          if(r == nullptr) {
            return MAKE_ERROR("Unable to create query");
          }
          Recordset result(r);

          ExecStatusType status = PQresultStatus(r);
          switch(status) {
            case PGRES_COMMAND_OK: {
              r = PQdescribePrepared(handle_, name);
              if(PQresultStatus(r) == PGRES_COMMAND_OK) {
                int n = PQnparams(r);
                std::vector<Oid> oids;
                for(int i = 0; i < n; i++) {
                  Oid oid = static_cast<Oid>(PQparamtype(r, i));
                  oids.push_back(oid);
                }
                preparedStmtOids_.insert_or_assign(name, oids);
              }
              PQclear(r);
              return Error::Success;
            }
            case PGRES_NONFATAL_ERROR:
            case PGRES_BAD_RESPONSE:
            case PGRES_FATAL_ERROR: {
              return MAKE_ERROR("Unable to execute query. %s", PQresultErrorMessage(r));
            }
            case PGRES_EMPTY_QUERY:
            case PGRES_TUPLES_OK:
            case PGRES_COPY_OUT:
            case PGRES_COPY_IN:
            case PGRES_COPY_BOTH:
            case PGRES_SINGLE_TUPLE:
            case PGRES_PIPELINE_SYNC:
            case PGRES_PIPELINE_ABORTED:
              return MAKE_ERROR("Unsupported query");
          }

          return Error::Success;
        }

        void execute(const char *preparedName, const QueryData *queryData, ExecuteHandler &&handler, RequestId requestId) {
          if(state_ != ConnectionImpl::State::Connected) {
            handler(MAKE_ERROR("Connection is currently disconnected"), {}, base_);
            return;
          }
          if(isBusy()) {
            handler(MAKE_ERROR("Connection is busy"), {}, base_);
            return;
          }

          int rc;
          if(queryData) {
            // check oids
            if(base_->options().isCheckQueryParameters()) {
              std::unordered_map<std::string, std::vector<Oid>>::const_iterator i = preparedStmtOids_.find(preparedName);
              if(i != preparedStmtOids_.end()) {
                const std::vector<Oid> &oids = i->second;
                if(queryData->count() != oids.size()) {
                  handler(MAKE_ERROR("Wrong parameter count."), {}, base_);
                  return;
                }
                for(size_t i = 0; i < oids.size(); i++) {
                  if(queryData->types()[i] != 0 && oids[i] != static_cast<Oid>(queryData->types()[i])) {
                    handler(MAKE_ERROR("Wrong parameter type %d for parameter %d. Must be %d.", queryData->types()[i], i, oids[i]), {}, base_);
                    return;
                  }
                }
              }
            }

            rc = PQsendQueryPrepared(handle_, preparedName, queryData->values().size(), queryData->values().data(), queryData->lengths().data(), queryData->formats().data(), 1);
          } else {
            rc = PQsendQueryPrepared(handle_, preparedName, 0, nullptr, nullptr, nullptr, 1);
          }
          if(rc == 0) {
            Error error = MAKE_ERROR("Unable to execute query. %s", PQerrorMessage(handle_));
            handler(error, {}, base_);
            return;
          }

          executeHandler_ = std::move(handler);
          requestId_ = requestId;
          isExecuting_ = true;
          return;
        }

        inline const ExecuteHandler &currentExecuteHandler() const {
          return executeHandler_;
        }
        inline RequestId currentRequestId() const {
          return requestId_;
        }

      private:
        AsyncObjectPtr<Connection> base_;
        PGconn *handle_ = nullptr;
        network::DnsResolver::RequestId dnsRequestId_ = {};
        int fd_ = -1;
        uv_poll_t *pollHandle_ = nullptr;
        int eventmask_ = 0;
        State state_ = State::Disconnected;
        ExecuteHandler executeHandler_;
        bool isExecuting_ = false;
        RequestId requestId_;
        AsyncObjectPtr<Timer> connectTimer_;
        std::unordered_map<std::string, std::vector<Oid>> preparedStmtOids_;
      };

      //----------------------------------------------------------
      Connection::Connection(EventLoop *eventLoop) : AsyncObject(eventLoop), reconnectTimer_(CONSTRUCT_ASYNC_OBJECT("Connection::reconnectTimer_"), eventLoop) {}

      Connection::~Connection() {
        destroy();
      }

      Error Connection::initialize(ConnectionId id, const Options &options, size_t hostIndex, ConnectedHandler &&connectedHandler, DisconnectedHandler &&disconnectedHandler) {
        destroy();
        id_ = id;
        options_ = options;
        hostIndex_ = hostIndex;
        connectedHandler_ = std::move(connectedHandler);
        disconnectedHandler_ = std::move(disconnectedHandler);
        if(options_.sslOptions().isAllow()) {
          Error error = sslTemporaryFiles_.certificate_.create(options_.sslOptions().certificatePemData());
          if(error.isFail()) {
            return MAKE_CHILD_ERROR(error, "Unable to initialize postgresql connection");
          }
          error = sslTemporaryFiles_.key_.create(options_.sslOptions().privateKeyPemData());
          if(error.isFail()) {
            return MAKE_CHILD_ERROR(error, "Unable to initialize postgresql connection");
          }
          if(!options_.sslOptions().trustedCertificatesPemData().empty()) {
            error = sslTemporaryFiles_.ca_.create(options_.sslOptions().trustedCertificatesPemData()[0]);
            if(error.isFail()) {
              return MAKE_CHILD_ERROR(error, "Unable to initialize postgresql connection");
            }
          }
          if(options_.userName().empty()) {
            options_.setUserName(core::crypto::utils::getX509CommonName(options_.sslOptions().certificatePemData()));
          }
        }

        connectionImpl_ = new ConnectionImpl(AsyncObjectPtr<Connection>(this));

        return Error::Success;
      }

      void Connection::destroy() {
        reconnectTimer_->stop();
        if(connectionImpl_) {
          connectionImpl_->disconnect();
          connectionImpl_ = nullptr;
          if(disconnectedHandler_) {
            disconnectedHandler_(Error::Success);
          }
        }
        connectedHandler_ = {};
        disconnectedHandler_ = {};
        options_ = {};
        id_ = {};
        sslTemporaryFiles_.clear();
      }

      bool Connection::isValid() const {
        return connectionImpl_ && connectionImpl_->state() == ConnectionImpl::State::Connected;
      }

      Error Connection::startReconnectTimer() {
        if(eventLoop()->state() != EventLoop::State::Running) {
          return Error::Success;
        }
        if(!reconnectTimer_->restart(options_.reconnectInterval(), [this]() {
             connectionImpl_ = new ConnectionImpl(AsyncObjectPtr<Connection>(this));
           })) {
          return MAKE_ERROR("Unable to start reconnect timer");
        }
        return Error::Success;
      }

      Error Connection::execute(const char *query, const QueryData *queryData, Recordset *result) {
        if(result) {
          result->clear();
        }
        if(!connectionImpl_) {
          return MAKE_ERROR("Connection is currently disconnected");
        }
        return connectionImpl_->execute(query, queryData, result);
      }

      Error Connection::prepare(const char *name, const char *query, const std::vector<unsigned int> *types) {
        if(!connectionImpl_) {
          return MAKE_ERROR("Connection is currently disconnected");
        }
        return connectionImpl_->prepare(name, query, types);
      }

      void Connection::execute(const char *preparedName, const QueryData *queryData, ExecuteHandler &&handler, RequestId requestId) {
        if(!connectionImpl_) {
          handler(MAKE_ERROR("Connection is currently disconnected"), {}, {});
          return;
        }
        return connectionImpl_->execute(preparedName, queryData, std::move(handler), requestId);
      }

      RequestId Connection::currentRequestId() const {
        if(!connectionImpl_) {
          return InvalidRequestId;
        }
        return connectionImpl_->currentRequestId();
      }

      const ExecuteHandler &Connection::currentExecuteHandler() const {
        if(!connectionImpl_) {
          static ExecuteHandler dummy;
          return dummy;
        }
        return connectionImpl_->currentExecuteHandler();
      }

      bool Connection::isBusy() const {
        return connectionImpl_ && connectionImpl_->isBusy();
      }

    } // namespace postgresql
  }   // namespace core
