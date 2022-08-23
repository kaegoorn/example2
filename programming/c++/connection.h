#pragma once
#include "core/microservice/asyncobject.h"
#include "core/microservice/timer.h"
#include "options.h"
#include "querydata.h"
#include "types.h"
#include <memory>

  namespace core {
    namespace postgresql {

      class Recordset;

      class Connection : public AsyncObject {
      public:
        using ConnectedHandler = std::function<Error()>;
        using DisconnectedHandler = std::function<void(const Error &error)>;

      public:
        virtual ~Connection();

        Error initialize(ConnectionId id, const Options &options, size_t hostIndex, ConnectedHandler &&connectedHandler, DisconnectedHandler &&disconnectedHandler);
        void destroy();

        bool isValid() const;
        bool isBusy() const;

        const Options &options() const;

        size_t hostIndex() const;
        const std::string &host() const;

        // asynchronous
        void execute(const char *preparedName, const QueryData *queryData, ExecuteHandler &&handler, RequestId requestId);

        // synchronous
        Error prepare(const char *name, const char *query, const std::vector<unsigned int> *types = nullptr);
        Error execute(const char *query, const QueryData *queryData = nullptr, Recordset *result = nullptr);

        ConnectionId id() const;

        const ExecuteHandler &currentExecuteHandler() const;
        RequestId currentRequestId() const;

        void setErrorHandler(ErrorHandler &&errorHandler);

      protected:
        Connection(EventLoop *eventLoop);
        friend AsyncObjectPtr<Connection>;

        Error startReconnectTimer();

      private:
        ConnectionId id_;
        Options options_;
        size_t hostIndex_;
        ConnectedHandler connectedHandler_;
        DisconnectedHandler disconnectedHandler_;
        class ConnectionImpl;
        ConnectionImpl *connectionImpl_ = nullptr;
        AsyncObjectPtr<Timer> reconnectTimer_;
        ByteArray userData_;

        class SslTmpFile {
        public:
          SslTmpFile() = default;
          ~SslTmpFile();
          Error create(std::string_view data);
          const std::string &path() const;
          void clear();

        private:
          SslTmpFile &operator=(const SslTmpFile &) = delete;
          std::string path_;
          std::FILE *f_ = nullptr;
        };

        struct SslTemporaryFiles {
          SslTmpFile certificate_;
          SslTmpFile key_;
          SslTmpFile ca_;
          void clear();
        } sslTemporaryFiles_;
      };

      inline ConnectionId Connection::id() const {
        return id_;
      }
      inline const Options &Connection::options() const {
        return options_;
      }
      inline size_t Connection::hostIndex() const {
        return hostIndex_;
      }
      inline const std::string &Connection::host() const {
        return options_.hosts()[hostIndex_];
      }

    } // namespace postgresql
  }   // namespace core
