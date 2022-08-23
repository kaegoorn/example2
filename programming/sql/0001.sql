/*
        ==================================== STRUCTURE ===========================================
*/

/*
        enum class OperationType {
                CreateAtomicgroup = 1,
                CreateInitialgroup = 2,
                CreateOrdinalgroup = 3,
                Createsession = 4,
                AddMemberToInitialgroup = 5,
                ...
        };

        Формат ENCRYPTED_DATA:
                        Идентификатор группы UUID             8 bytes (4 bytes time_and_version, 4 bytes clock_seq_and_node)
                        Идентификатор сессии INTEGER          4 bytes
                        Данные                                      ...

*/

/*
        database_schema_version
        Таблица, содержащая порядковый номер миграции
*/
CREATE TABLE database_schema_version (
        schema_version                       INTEGER,                                    -- версия схемы
        upgraded                             TIMESTAMP                                   -- время обновления
                                             DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(schema_version)
);


/*
        groups
        Таблица, содержащая базовую информацию о группах
*/
CREATE TABLE groups (
        group_id                            UUID                                   -- идентификатор группы
                                                                        NOT NULL,
        type                                      SMALLINT                               -- 1: атомарная, в эту группу не могут входить другие группы, 2: initial, в эту группу могут входить только атомарные, 3: ordinal, обычная группа
                                                                        NOT NULL,
        status                                    SMALLINT                               -- статус группы: 1 - активна
                                                                        DEFAULT 1,
        PRIMARY KEY(group_id)
);
/*
        запрещаем удаление и изменение записей в таблице групп
*/
-- CREATE RULE groups_update_protect AS ON UPDATE TO groups DO INSTEAD NOTHING;
-- CREATE RULE groups_delete_protect AS ON DELETE TO groups DO INSTEAD NOTHING;



/*
        create_initial_group_v1
        Создание начальной группы.

*/
CREATE FUNCTION create_initial_group_v1(
                                                _group_id                      UUID,
                                                _atomic_group_id               UUID,
                                                _public_key                         VARCHAR,
                                                _encrypted_private_key               VARCHAR,
                                                _member_ids                          UUID[],
                                                _encrypted_symmetric_keys            VARCHAR[],
                                                _service_data_ids                    VARCHAR[],
                                                _service_data_keys                   VARCHAR[],
                                                _service_data_values                 VARCHAR[],
                                                _raw_json                            VARCHAR,
                                                _signature                           VARCHAR
                                            ) RETURNS VOID
AS $BODY$
DECLARE
BEGIN
    PERFORM create_group_v1(
        _group_id,
        2,
        _atomic_group_id,
        _public_key,
        _encrypted_private_key,
        _member_ids,
        _encrypted_symmetric_keys,
        _service_data_ids,
        _service_data_keys,
        _service_data_values,
        _raw_json,
        _signature);
END;
$BODY$
LANGUAGE plpgsql;
