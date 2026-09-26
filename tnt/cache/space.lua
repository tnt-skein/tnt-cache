--- Драйвер на спейсе Tarantool: кэш переживает перезапуск и виден
--- всем репликам.
---
--- Спейс заводится шагом миграции (`space.migrate(box)`), а драйвер только
--- читает и пишет. Запись — «ключ, значение, срок, теги»; значение обязано
--- укладываться в msgpack. На реплике только для чтения запись отказывает,
--- как и всякая запись в спейс, — это тот же договор, что у данных.
---
--- Обход для `sweep` — партии по `GT` с `limit` от последнего ключа
--- куска, а не итератор на весь спейс: итератор через уступку
--- не держится, а куски без уступки срез файбера не спасает.
--- Продолжение — по первичному ключу, он уникален, и `GT` ничего
--- не перепрыгивает. Первичный индекс обязан быть TREE: у HASH
--- `GT` от только что стёртого ключа отдаёт пустоту (проверено на 3.8),
--- и обход обрывался бы на первом же куске со стёртым последним ключом.
--- Обход — не снимок: ключ, дописанный соседом впереди позиции, обход
--- увидит, позади — нет; стёртый впереди — пропустит. Просроченное
--- из куска стирается одной транзакцией: 5000 стираний по одному —
--- 5000 транзакций и 65 мс, партиями по тысяче — 5 и 12 мс (3.8).
---
--- **Теги — второй спейс `<имя>_tags`**: строка «тег, ключ» на каждый тег
--- записи, первичный TREE по обоим полям. Забывание тега выбирает строки
--- тега куском `EQ` с `limit` и стирает записи вместе со строками одной
--- транзакцией; следующий кусок снова берётся с начала тега — стёртых
--- в нём уже нет. Индекс по элементам массива в самом спейсе кэша был бы
--- проще, но с `memtx_use_mvcc_engine` ядро его не заводит («Memtx MVCC
--- engine does not support multikey indexes», 3.8), и шаг миграции
--- падал бы на таких узлах. Запись помнит свои теги четвёртым полем:
--- перезапись и стирание вынимают по нему прежние строки тегов в той же
--- транзакции, и тег знает ровно живые записи с ним. Поле читается
--- по номеру, а не по имени: у спейса, заведённого до тегов, у четвёртого
--- поля имени нет. Нет спейса тегов — тегов драйвер не знает, а пишет
--- по-прежнему одной командой.
---
--- **Транзакция — своя либо вызывающего.** `box.atomic` внутри чужой
--- транзакции отказывает («Operation is not permitted when there is
--- an active transaction»), а запись в кэш из транзакции прикладного кода —
--- обычное дело; тогда она становится её частью и откатывается вместе
--- с ней, как одиночная замена.
---
--- **Положить, если нет (`add`), — вставкой.** Чтение с последующей
--- заменой отдало бы замок двоим: с `memtx_use_mvcc_engine` чтение
--- не видит чужой вставки, пока та пишется в WAL, — на 3.8 пять файберов
--- из пяти взяли замок чтением с заменой, и ровно один — вставкой.
--- Вставку уже вставленного ключа ядро отвергает и с MVCC, и без него.
---
--- **Спейс по имени ищется первым обращением, а не при сборке**
--- (`named`). Кэш собирают при применении роли, а спейс заводит шаг
--- миграции, который ядро поднимает событием после применения: драйвер,
--- искавший спейс сразу, на пустой базе ронял бы применение, и шаг,
--- который завёл бы спейс, не пошёл бы никогда. Пока спейса нет, каждое
--- обращение отказывает отказом box «спейса нет» с именем шага.

local external = require('tnt.external')

--- Текст отказа «тегов не знает» — общий с видом хранилища: запись
--- с тегами, которую не пустил драйвер по имени, и вид, который не завёлся,
--- говорят об одном и том же одними словами.
local tagged = require('tnt.cache.tagged')

--- Отказ сборки — словом, без места в коде; чужая поломка, брошенная
--- заново, уходит как есть.
local fail = require('tnt.must.fail').raise

local Module = {}

--- Имя спейса, если не задано иное.
Module.SPACE = 'app_cache'

--- Окончание имени спейса тегов: `app_cache` → `app_cache_tags`.
Module.TAGS = '_tags'

--- Вид записи: ключ, значение, срок и теги.
---
--- Теги — необязательное поле: запись без тегов остаётся тройкой.
local FORMAT = {
    { name = 'key', type = 'string' },
    { name = 'value', type = 'any' },
    { name = 'expires_at', type = 'number' },
    { name = 'tags', type = 'array', is_nullable = true },
}

--- Вид строки тега.
local TAGS_FORMAT = {
    { name = 'tag', type = 'string' },
    { name = 'key', type = 'string' },
}

--- Номер поля тегов в записи.
local TAGS_FIELD = 4

--- Код отказа «такой ключ уже есть». Кодов отказов в аннотациях ядра нет.
---@diagnostic disable-next-line: undefined-field
local DUPLICATE = box.error.TUPLE_FOUND

--- Отказ, пока шаг миграции не завёл спейс.
local MISSING = 'кэш: спейса %s нет — нужен шаг миграции tnt.cache.space.migrate'

--- Внешние средства: транзакция box и спейс по имени.
---
--- Через внешнюю зависимость затем, чтобы проверки над двойником спейса шли без
--- поднятого инстанса: до `box.cfg` поле `atomic` у box бросает, а спейсов
--- нет вовсе — и проверка драйвера по имени заводит их сама.
local source = external.install(Module, {
    atomic = function(fn, ...)
        if box.is_in_txn() then
            return fn(...)
        end

        return box.atomic(fn, ...)
    end,

    space = function(name)
        return box.space[name]
    end,
})

--- Шаг миграции: заводит спейс кэша с индексом по ключу и спейс его тегов.
---
--- Повторно безопасен: чего нет — заводит, что есть — не трогает. Так
--- спейс, заведённый до тегов, доводится тем же шагом в новой миграции:
--- к нему дописывается пустой спейс тегов, а записи остаются.
---@param box table
---@param name string|nil
function Module.migrate(box, name)
    local space_name = name or Module.SPACE
    local tags_name = space_name .. Module.TAGS

    if box.space[space_name] == nil then
        local space = box.schema.space.create(space_name, { format = FORMAT })

        space:create_index('primary', { parts = { 'key' } })
    end

    if box.space[tags_name] == nil then
        local tags = box.schema.space.create(tags_name, { format = TAGS_FORMAT })

        tags:create_index('primary', { parts = { 'tag', 'key' } })
    end
end

--- Стирает партию ключей. Зовётся внутри транзакции.
---@param space table
---@param keys string[]
local function delete_batch(space, keys)
    for _, key in ipairs(keys) do
        space:delete(key)
    end
end

--- Вставка: свободный ключ — `true`, занятый — `false`.
---
--- Занято — ответ, а не отказ; всё прочее (реплика, снесённый спейс) —
--- отказ записи, и он уходит как есть.
---@param put fun(...): any Чем вставлять
---@param ... any
---@return boolean
local function inserted(put, ...)
    local ok, failure = pcall(put, ...)

    if ok then
        return true
    end

    ---@cast failure any

    if failure.code ~= DUPLICATE then
        fail(failure)
    end

    return false
end

--- Драйвер без тегов: каждое действие — одна команда спейса.
---@param space table
---@return TntCacheDriver
local function plain(space)
    return {
        name = 'space',
        read = function(key)
            local tuple = space:get(key)

            if tuple == nil then
                return nil
            end

            return tuple.value, tuple.expires_at, tuple[TAGS_FIELD]
        end,
        write = function(key, value, expires_at, tags)
            space:replace({ key, value, expires_at, tags })
        end,
        insert = function(key, value, expires_at, tags)
            return inserted(space.insert, space, { key, value, expires_at, tags })
        end,
        erase = function(key)
            space:delete(key)
        end,
        erase_many = function(keys)
            source().atomic(delete_batch, space, keys)
        end,
        clear = function()
            space:truncate()
        end,
        scan = function(cursor, limit)
            local key, iterator = {}, 'GE'

            if cursor ~= nil then
                key, iterator = { cursor }, 'GT'
            end

            local tuples = space:select(key, { iterator = iterator, limit = limit })

            if #tuples < limit then
                return tuples, nil
            end

            return tuples, tuples[#tuples].key
        end,
    }
end

--- Заводит драйвер над спейсом.
---@param space table Спейс box.space[...]
---@param tags table|nil Спейс тегов; нет его — тегов драйвер не знает
---@return TntCacheDriver
function Module.new(space, tags)
    if type(space) ~= 'table' then
        fail('кэш: драйверу space нужен спейс')
    end

    if space.index[0].type ~= 'TREE' then
        fail(
            'кэш: драйверу space нужен спейс с первичным индексом TREE — по HASH обход от стёртого ключа обрывается'
        )
    end

    local driver = plain(space)

    if tags == nil then
        driver.tagless = ('нет спейса тегов %s%s — его заведёт шаг миграции tnt.cache.space.migrate'):format(
            tostring(space.name),
            Module.TAGS
        )

        return driver
    end

    --- Вынимает стёртую либо переписанную запись из строк её тегов.
    ---@param tuple any Прежняя запись; пусто — её не было
    local function untag(tuple)
        if tuple == nil then
            return
        end

        for _, tag in ipairs(tuple[TAGS_FIELD] or {}) do
            tags:delete({ tag, tuple[1] })
        end
    end

    --- Кладёт строки тегов записи.
    ---@param key string
    ---@param list string[]|nil
    local function retag(key, list)
        for _, tag in ipairs(list or {}) do
            tags:replace({ tag, key })
        end
    end

    --- Стирает записи вместе со строками их тегов. Зовётся в транзакции.
    ---@param keys string[]
    ---@return integer erased Сколько записей было
    local function drop(keys)
        local erased = 0

        for _, key in ipairs(keys) do
            local old = space:delete(key)

            if old ~= nil then
                erased = erased + 1
            end

            untag(old)
        end

        return erased
    end

    driver.write = function(key, value, expires_at, list)
        source().atomic(function()
            untag(space:get(key))
            space:replace({ key, value, expires_at, list })
            retag(key, list)
        end)
    end

    driver.insert = function(key, value, expires_at, list)
        return inserted(source().atomic, function()
            space:insert({ key, value, expires_at, list })
            retag(key, list)
        end)
    end

    driver.erase = function(key)
        source().atomic(drop, { key })
    end

    driver.erase_many = function(keys)
        source().atomic(drop, keys)
    end

    driver.clear = function()
        space:truncate()
        tags:truncate()
    end

    driver.erase_tagged = function(tag, limit)
        local keys = {}

        for index, row in ipairs(tags:select({ tag }, { iterator = 'EQ', limit = limit })) do
            keys[index] = row[2]
        end

        if #keys == 0 then
            return 0, 0
        end

        -- Строка тега стирается и тогда, когда записи уже нет: иначе она
        -- попадалась бы каждому следующему куску, и тег не кончился бы.
        return #keys,
            source().atomic(function()
                for _, key in ipairs(keys) do
                    tags:delete({ tag, key })
                end

                return drop(keys)
            end)
    end

    return driver
end

--- Отказ «спейса нет» — отказом box с его кодом, а не строкой.
---
--- Спейса, который шаг ещё не завёл, нет так же, как снесённого, и `sweep`
--- отдаёт такой отказ парой, как всякий отказ box посреди обхода, а не
--- бросает его поломкой драйвера. Кодов отказов в аннотациях ядра нет.
---@param name string
---@return any
local function missing(name)
    ---@type any
    local errors = box.error

    return errors.new({ code = errors.NO_SUCH_SPACE, reason = MISSING:format(name) })
end

--- Заводит драйвер над спейсом по имени: спейс ищется первым обращением.
---
--- Сборка box не трогает, и кэш по имени заводят и при применении роли,
--- и при её загрузке, когда box ещё не поднят. Пока спейса нет, каждое
--- обращение отказывает с именем шага миграции: чтение хранилище считает
--- промахом, запись бросает, `sweep` отдаёт парой. Найденный драйвер
--- помнится, а спейс тегов ищется, пока его нет: спейс, заведённый
--- до тегов, шаг доводит уже на живом узле, и теги появляются первым
--- обращением после шага, без перечитывания конфигурации.
---
--- Есть ли теги, до первого обращения не знает никто, и вид с тегами над
--- таким драйвером заводится всегда. Тег, который потом не забыть, при
--- этом не ложится: пока спейса тегов нет, запись с тегами и забывание
--- тега отказывают тем же словом, каким отказал бы сам вид.
---@param name string Имя спейса; спейс тегов — `<имя>_tags`
---@return TntCacheDriver
function Module.named(name)
    local tags_name = name .. Module.TAGS

    ---@type TntCacheDriver|nil
    local current

    --- Драйвер над найденными спейсами.
    ---@return TntCacheDriver
    local function resolved()
        -- Драйвер с тегами — окончательный: искать больше нечего.
        if current ~= nil and current.erase_tagged ~= nil then
            return current
        end

        local found = source().space(name)

        if found == nil then
            fail(missing(name))
        end

        local tags = source().space(tags_name)

        -- Драйвер без тегов заводится заново, только когда спейс тегов
        -- появился: прежний иначе годится как есть.
        if current == nil or tags ~= nil then
            current = Module.new(found, tags)
        end

        ---@cast current TntCacheDriver
        return current
    end

    --- Найденный драйвер, которому по пути нужны теги либо нет.
    ---@param wanted boolean Пишутся ли теги
    ---@return TntCacheDriver
    local function tagging(wanted)
        local driver = resolved()

        if wanted and driver.erase_tagged == nil then
            fail(tagged.TAGLESS:format(driver.name, driver.tagless))
        end

        return driver
    end

    return {
        name = 'space',
        read = function(key)
            return resolved().read(key)
        end,
        write = function(key, value, expires_at, list)
            tagging(list ~= nil).write(key, value, expires_at, list)
        end,
        insert = function(key, value, expires_at, list)
            -- Вставка у драйвера на спейсе есть всегда, с тегами и без.
            local insert = tagging(list ~= nil).insert

            ---@cast insert -?
            return insert(key, value, expires_at, list)
        end,
        erase = function(key)
            resolved().erase(key)
        end,
        erase_many = function(keys)
            resolved().erase_many(keys)
        end,
        clear = function()
            resolved().clear()
        end,
        scan = function(cursor, limit)
            return resolved().scan(cursor, limit)
        end,
        erase_tagged = function(tag, limit)
            -- Без забывания тега `tagging` не отдаёт драйвер вовсе.
            local erase = tagging(true).erase_tagged

            ---@cast erase -?
            return erase(tag, limit)
        end,
    }
end

return Module
