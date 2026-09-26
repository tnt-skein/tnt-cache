--- Вид хранилища с тегами: что положено через него, помечено тегами.
---
--- **Теги — пометка записи, а не часть ключа.** `store:tags({ 'docs' })`
--- отдаёт вид хранилища, чьи `put`, `add` и `remember` помечают запись
--- тегами, а `flush` забывает все записи с любым из них. Читают по-прежнему
--- по ключу у самого хранилища: ключ от тегов не зависит, и запись,
--- положенная с тегом, видна `get` без него. Где держать указатель
--- «тег → ключи», решает драйвер (`erase_tagged`), а вид зовёт его
--- кусками с уступкой перед каждым, как `sweep`, — тег на сотни тысяч
--- записей не упирается в срез файбера и не держит соседей. Теги
--- с приставкой хранилища: `flush` тега одного хранилища записи соседа
--- с другой приставкой не трогает. Договор драйвера:
--- `erase_tagged(tag, limit) → taken, erased` стирает до `limit` записей
--- с тегом и отвечает, сколько ключей взято из указателя тега и сколько
--- записей из них было; взято меньше `limit` — тег кончился. Нет его —
--- тегов драйвер не знает, и `tagless` говорит почему.
---
---     local docs = store:tags({ 'docs' })
---
---     docs:put('docs:7', page, 300)
---     docs:remember('docs:index', 300, render)
---     docs:flush()                                     --> сколько забыто
---
--- Хранилище приходит сюда аргументом: `Store:tags` — тонкий вход, а
--- запись через драйвер вид берёт у `tnt.cache.record`, счёт одним
--- на всех — у `tnt.cache.lock`.

local lock = require('tnt.cache.lock')
local record = require('tnt.cache.record')

--- Бросок без места в коде: у отказа настройки места нет.
local fail = require('tnt.must.fail').raise

---@class TntCacheTagged Вид хранилища: что положено через него, помечено тегами
---@field store TntCacheStore
---@field names string[] Теги с приставкой хранилища, без повторов
local Tagged = {}
Tagged.__index = Tagged

--- Тексты отказов тегов.
local BAD_TAGS =
    'кэш: теги — непустая строка либо непустой список непустых строк'
local TAGLESS = 'кэш: драйвер %s тегов не знает: %s'
local TAGS_IN_TRANSACTION =
    'кэш: забывание тега уступает управление и само фиксирует партии — внутри транзакции его не зовут'

local Module = {}

--- Внешние средства — общие с хранилищем: подмена в проверке одна.
local source = record.source

--- Теги с приставкой хранилища, без повторов.
---@param store TntCacheStore
---@param names any Тег либо список тегов
---@return string[]
local function tag_list(store, names)
    if type(names) == 'string' then
        names = { names }
    end

    if type(names) ~= 'table' or #names == 0 then
        fail(BAD_TAGS)
    end

    local seen, list = {}, {}

    for _, name in ipairs(names) do
        if type(name) ~= 'string' or name == '' then
            fail(BAD_TAGS)
        end

        -- Повтор отбрасывается здесь, а не у драйвера: указатель тега
        -- у драйвера в памяти вынимал бы ключ из одного множества дважды.
        if seen[name] == nil then
            seen[name] = name
            table.insert(list, store.prefix .. name)
        end
    end

    return list
end

--- Вид хранилища, помечающий записи тегами, — тело `Store:tags`.
---
--- Драйвер без `erase_tagged` тегов не знает, и это ошибка программиста
--- уже здесь, а не на забывании: тег, положенный туда, где его потом
--- не забыть, оставил бы устаревшее значение жить до срока.
---@param store TntCacheStore
---@param names string|string[] Тег либо список тегов
---@return TntCacheTagged
local function new(store, names)
    local driver = store.driver

    if driver.erase_tagged == nil then
        fail(TAGLESS:format(driver.name, driver.tagless or 'у него нет erase_tagged'))
    end

    return setmetatable({ store = store, names = tag_list(store, names) }, Tagged)
end

--- Кладёт значение с тегами вида.
---@param key string
---@param value any
---@param ttl number|nil
function Tagged:put(key, value, ttl)
    record.stored(self.store, key, value, ttl, self.names)
end

--- Кладёт с тегами вида, только если живого значения нет.
---@param key string
---@param value any
---@param ttl number|nil
---@return boolean added
function Tagged:add(key, value, ttl)
    return record.placed(self.store, key, value, ttl, self.names)
end

--- Запоминает значение с тегами вида; настройки — как у `Store:remember`.
---@param key string
---@param ttl number|nil
---@param compute fun(): any
---@param opts TntCacheRememberOptions|nil
---@return any value
---@return string|nil err
function Tagged:remember(key, ttl, compute, opts)
    return lock.remember(self.store, key, ttl, compute, self.names, opts)
end

--- Забывает все записи с любым из тегов вида — кусками, с уступкой
--- перед каждым; ответ — сколько записей стёрто.
---
--- Стёртое до отказа остаётся стёртым, а отказ бросает, как всякая
--- запись: тег, забытый наполовину, оставил бы устаревшее значение,
--- и молчать об этом нельзя. Внутри транзакции не зовут: уступка её
--- оборвёт. Забывают после фиксации правки: забытое до неё успел бы
--- посчитать заново сосед — по прежним данным.
---@return integer erased
function Tagged:flush()
    -- Вид заводится только над драйвером с `erase_tagged` (`new`).
    local erase = self.store.driver.erase_tagged
    local chunk = record.CHUNK
    local erased = 0

    ---@cast erase fun(tag: string, limit: integer): integer, integer

    if source().in_transaction() then
        fail(TAGS_IN_TRANSACTION)
    end

    for _, tag in ipairs(self.names) do
        repeat
            source().yield()

            local taken, count = erase(tag, chunk)

            erased = erased + count
        until taken < chunk
    end

    return erased
end

Module.new = new

--- Текст отказа «тегов не знает» — наружу: драйвер на спейсе по имени
--- отказывает им записи с тегами, пока шаг миграции не завёл спейс тегов.
Module.TAGLESS = TAGLESS

return Module
