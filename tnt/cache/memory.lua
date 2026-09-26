--- Драйвер в памяти процесса: таблица ключ → запись.
---
--- Годится на один узел и на время его жизни: перезапуск — пустой кэш,
--- сосед по кластеру своего не видит. Значения хранятся как есть,
--- без копии: таблица, положенная в кэш и потом изменённая, изменится
--- и в кэше — это цена скорости, и об этом стоит помнить.
---
--- Обход для `sweep` идёт по снимку ключей, снятому первым куском:
--- порядка у таблицы Lua нет, а продолжать `next` с ключа, стёртого
--- до перестройки таблицы, нельзя — LuaJIT бросает «invalid key to
--- 'next'» (проверено на 3.8: после стирания ключа и тысячи вставок).
--- Снимок — только ключи, значения не копируются. Ключ, стёртый после
--- снимка, обход пропускает; дописанный после — достанется следующему
--- обходу. Снимок снимается без уступки: это счёт по таблице в памяти,
--- срез файбера его не касается.
---
--- **Теги — указатель «тег → ключи» рядом с записями.** Запись помнит
--- свои теги, и всякое её стирание и всякая перезапись вынимают ключ
--- из указателя: тег знает ровно живые записи с ним, и забывание тега
--- стирает только их, не обходя остальной кэш. Пустое множество тега
--- уходит из указателя вместе с последним ключом — имена тегов память
--- не копят. Кусок забывания берётся заново с начала множества: стёртые
--- ключи из него уже вынуты, и продолжать `next` с них не приходится.

local Module = {}

--- Заводит драйвер.
---@return TntCacheDriver
function Module.new()
    ---@type table<string, { value: any, expires_at: number, tags: string[]|nil }>
    local entries = {}

    --- Тег → множество ключей с ним (ключ → сам ключ).
    ---@type table<string, table<string, string>>
    local tagged = {}

    --- Стирает запись и вынимает её ключ из указателя тегов.
    ---@param key string
    local function drop(key)
        local entry = entries[key]

        if entry == nil then
            return
        end

        entries[key] = nil

        for _, tag in ipairs(entry.tags or {}) do
            local keys = tagged[tag]

            keys[key] = nil

            if next(keys) == nil then
                tagged[tag] = nil
            end
        end
    end

    return {
        name = 'memory',
        read = function(key)
            local entry = entries[key]

            if entry == nil then
                return nil
            end

            return entry.value, entry.expires_at, entry.tags
        end,
        write = function(key, value, expires_at, tags)
            -- Перезапись заменяет и теги: запись без тега, положенная
            -- поверх помеченной, из множества тега уходит.
            drop(key)
            entries[key] = { value = value, expires_at = expires_at, tags = tags }

            for _, tag in ipairs(tags or {}) do
                local keys = tagged[tag] or {}

                keys[key] = key
                tagged[tag] = keys
            end
        end,
        erase = drop,
        erase_many = function(keys)
            for _, key in ipairs(keys) do
                drop(key)
            end
        end,
        erase_tagged = function(tag, limit)
            local keys = tagged[tag] or {}
            local batch = {}
            local key = next(keys)

            -- Кусок собирается целиком до стирания: стирание вынимает
            -- ключи из того же множества, по которому идёт `next`.
            while key ~= nil and #batch < limit do
                table.insert(batch, key)
                key = next(keys, key)
            end

            for _, each in ipairs(batch) do
                drop(each)
            end

            return #batch, #batch
        end,
        clear = function()
            entries = {}
            tagged = {}
        end,
        scan = function(cursor, limit)
            if cursor == nil then
                local keys = {}

                for key in pairs(entries) do
                    table.insert(keys, key)
                end

                cursor = { keys = keys }
            end

            local keys = cursor.keys
            local position = cursor.position
            local batch = {}

            -- По снимку — `next`: снимок не меняется, и продолжать с его
            -- позиции можно; кончился снимок — кончился обход.
            while #batch < limit do
                local index, key = next(keys, position)

                if index == nil then
                    return batch, nil
                end

                ---@cast key string

                position = index

                local entry = entries[key]

                if entry ~= nil then
                    table.insert(batch, { key = key, expires_at = entry.expires_at })
                end
            end

            return batch, { keys = keys, position = position }
        end,
    }
end

return Module
