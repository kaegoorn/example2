#pragma once

#include <functional>
#include <map>
#include <string>

#include "basicvalue.h"
#include "core/common/error.h"
#include "core/common/flags.h"
#include "deserializer.h"
#include "serializer.h"

  namespace core {
    namespace serializers {

      class Serializable {
      public:
        enum class Flag {
          Default = 0,
          RawJson = 1
        };

        using Flags = core::Flags<Flag>;

      public:
        virtual ~Serializable();
        virtual Error serialize(Serializer *serializer, std::string_view name) const;
        virtual Error deserialize(Deserializer *deserializer, bool skipStartTag = false);

      protected:
        struct SerializableMemberInfo {
          using Setter = std::function<bool(Serializable *, const BasicValue &)>;
          using SerializeHandler = std::function<Error(const Serializable *_this, Serializer *serializer, std::string_view name)>;
          using DeserializeHandler = std::function<Error(Serializable *_this, Deserializer *deserializer)>;
          std::string name;
          SerializeHandler serializeHandler;
          DeserializeHandler deserializeHandler;
          template <typename T, typename M, typename CastTo = M>
          SerializableMemberInfo(std::string_view name, M T::*member, const M &defaultValue = {}, Flags flags = Flag::Default, CastTo cast = {});
          SerializableMemberInfo(std::string_view name, SerializeHandler &&serializeHandler, DeserializeHandler &&deserializeHandler);
        };

        struct SerializableMembers {
          SerializableMembers(const std::vector<SerializableMemberInfo> &members);
          std::vector<SerializableMemberInfo> binds;
          std::map<std::string, size_t, std::less<>> index;
          SerializableMembers operator+(const SerializableMembers &other) const;
        };

        virtual const SerializableMembers &getBindings() const = 0;
      };

      template <typename T, typename M, typename CastTo>
      Serializable::SerializableMemberInfo::SerializableMemberInfo(std::string_view name, M T::*member, const M &defaultValue, Flags flags, CastTo) :
          name(name),
          serializeHandler([member](const Serializable *_this, Serializer *serializer, std::string_view name) -> Error {
            return serializer->serialize(name, static_cast<CastTo>(reinterpret_cast<const T *>(_this)->*member));
          }),
          deserializeHandler([member, defaultValue, flags](Serializable *_this, Deserializer *deserializer) -> Error {
            if constexpr(std::is_base_of<std::string, M>::value) {
              if(flags & Flag::RawJson) {
                Deserializer::OperationResult result = deserializer->deserializeNext();
                if(result.status() == Deserializer::OperationResult::Status::Fail) {
                  return MAKE_CHILD_ERROR(deserializer->getLastError(), "Unable to deserialize");
                }
                if(result.status() != Deserializer::OperationResult::Status::StartGroup) {
                  return MAKE_CHILD_ERROR(deserializer->getLastError(), "Service data must be object");
                }
                size_t start = deserializer->currentPosition();
                std::map<std::string, bool> dummy;
                Error error = deserializer->deserializeGroup(
                    dummy,
                    [](std::map<std::string, bool> &out, std::string_view name) -> bool * {
                      std::pair<std::map<std::string, bool>::iterator, bool> i = out.emplace(std::string(name), false);
                      return &i.first->second;
                    },
                    Deserializer::DeserializeItemOptions(Deserializer::DeserializeItemOption::SkipItem) | Deserializer::DeserializeItemOption::SkipStartTag);
                if(error.isFail()) {
                  return MAKE_CHILD_ERROR(error, "Unable to deserialize group");
                }
                size_t end = deserializer->currentPosition();
                reinterpret_cast<T *>(_this)->*member = std::string(reinterpret_cast<const char *>(deserializer->sourceData().data()), start, end - start);
                return Error::Success;
              }
            } else {
              std::ignore = flags;
            }
            if constexpr(std::is_same<M, CastTo>::value) {
              return deserializer->deserialize(reinterpret_cast<T *>(_this)->*member, &defaultValue);
            } else {
              CastTo v;
              CastTo def = static_cast<CastTo>(defaultValue);
              Error error = deserializer->deserialize(v, &def);
              if(error.isSuccess()) {
                reinterpret_cast<T *>(_this)->*member = static_cast<M>(v);
              }
              return error;
            }
          }) {}

      inline Serializable::SerializableMemberInfo::SerializableMemberInfo(std::string_view _name, SerializeHandler &&_serializeHandler, DeserializeHandler &&_deserializeHandler) :
          name(_name), serializeHandler(std::move(_serializeHandler)), deserializeHandler(std::move(_deserializeHandler)) {}

      DECLARE_FLAG_OPERATORS(Serializable::Flags);

#define DECLARE_SERIALIZED_MEMBERS(...)                                                                                                                                            \
protected:                                                                                                                                                                         \
  const SerializableMembers &getBindings() const override {                                                                                                                        \
    static SerializableMembers bindings = {__VA_ARGS__};                                                                                                                           \
    return bindings;                                                                                                                                                               \
  }

#define DECLARE_SERIALIZED_MEMBERS_INHERITED(parent, ...)                                                                                                                          \
protected:                                                                                                                                                                         \
  const SerializableMembers &getBindings() const override {                                                                                                                        \
    static SerializableMembers bindings = parent::getBindings() + SerializableMembers(__VA_ARGS__);                                                                                \
    return bindings;                                                                                                                                                               \
  }

    } // namespace serializers
  }   // namespace core
