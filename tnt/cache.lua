--- Кэш приложения: значения по ключу на срок.
---
---     local cache = require('tnt.cache')
---
---     local store = cache.new({ driver = 'memory', prefix = 'demo:', ttl = 60 })
---
---     store:put('customers:7', customer, 300)
---     store:get('customers:7')                 -- либо nil
---     store:remember('roles', 60, function() return compute() end)
---     store:forget('customers:7')
---     store:increment('hits')
---
---     store:tags({ 'docs' }):put('docs:7', page, 300)    -- запись с тегом
---     store:tags({ 'docs' }):flush()                     -- забыть все записи тега
---     store:remember('docs:index', 300, render, { lock = 30 }) -- считает один, прочие ждут
---     local lock, err = store:lock('reports', 30, { wait = 5 }) -- замок на ключ
---     lock:release()
---
--- Драйвера три: `memory` — таблица в процессе, быстро и на один узел;
--- `space` — спейс Tarantool, переживает перезапуск и виден репликам,
--- спейс заводится шагом миграции `require('tnt.cache.space').migrate(box)`,
--- а драйвер по имени ищет его первым обращением, а не при сборке;
--- `redis` — Redis через клиент `tnt-redis`, пришедший аргументом `redis`:
--- один кэш на все узлы, срок ведёт сам сервер.
--- Просроченное читается как отсутствующее и стирается по дороге;
--- `sweep()` чистит заранее — кусками с уступкой, партиями по `GT`
--- с `limit`, чтобы обход большого спейса не упирался в срез файбера.
---
--- Теги — пометка записи: забывание тега стирает только свои записи,
--- не обходя остальной кэш, — у каждого драйвера свой указатель «тег →
--- ключи». Замок на ключ — запись со сроком и меткой держателя; снимает
--- его только взявший.
---
--- Обращения за значением видны рядами метрик: `cache_requests_total`
--- по хранилищу и итогу — `hit`, `miss`, `failed` — и длительность чтения
--- драйвером. Хранилище в метке — по имени `name`, по умолчанию имя
--- драйвера: `cache.new({ name = 'pages', driver = 'redis', redis = client })`.
---
--- Части: `store` — хранилище: срок, приставка, счётчик и обход;
--- `record` — запись через драйвер, общая для всех частей; `lock` —
--- замок на ключ и счёт одним на всех; `tagged` — вид с тегами;
--- `series` — ряды метрик; `memory`, `space` и `redis` — драйверы.

local memory = require('tnt.cache.memory')
local redis = require('tnt.cache.redis')
local space = require('tnt.cache.space')
local store = require('tnt.cache.store')

--- Отказ настройки — словом, без места в коде.
local raise = require('tnt.must.fail').raise

local Module = {}

Module.memory = memory
Module.redis = redis
Module.space = space
Module.store = store

---@class TntCacheOptions
---@field name string|nil Имя хранилища в рядах метрик; по умолчанию — имя драйвера
---@field driver string|TntCacheDriver|nil `memory` (по умолчанию), `space`, `redis` либо свой драйвер
---@field space string|table|nil Имя спейса либо сам спейс — для драйвера `space`; спейс тегов — `<имя>_tags`
---@field redis TntCacheRedisClient|nil Клиент tnt-redis — для драйвера `redis`
---@field namespace string|nil Пространство ключей в Redis — для драйвера `redis`; по умолчанию `cache:`
---@field prefix string|nil Приставка ключей
---@field ttl number|nil Срок по умолчанию, секунды
---@field now (fun(): number)|nil Часы — для проверок

--- Драйвер по имени либо как дан.
---@param opts TntCacheOptions
---@return TntCacheDriver
local function driver_of(opts)
    local declared = opts.driver or 'memory'

    if type(declared) == 'table' then
        return declared
    end

    if declared == 'memory' then
        return memory.new()
    end

    if declared == 'redis' then
        return redis.new(opts.redis, { namespace = opts.namespace })
    end

    -- Отказ стоит условием, а не последней строкой: бросок помощником
    -- разбор типов концом функции не считает, и функция кончается
    -- возвратом драйвера.
    if declared ~= 'space' then
        raise(('кэш: драйвера %s нет, есть memory, space и redis'):format(tostring(declared)))
    end

    local given = opts.space or space.SPACE

    -- Спейс по имени драйвер ищет сам, первым обращением: кэш собирают
    -- при применении роли, а спейс заводит шаг миграции уже после него.
    if type(given) == 'string' then
        return space.named(given)
    end

    -- Спейс тегов — по имени рядом: `app_cache` → `app_cache_tags`. У спейса
    -- без имени (двойник в проверках) тегов нет.
    return space.new(given, given.name and box.space[given.name .. space.TAGS])
end

--- Заводит хранилище.
---@param opts TntCacheOptions|nil
---@return TntCacheStore
function Module.new(opts)
    opts = opts or {}

    return store.new({
        name = opts.name,
        driver = driver_of(opts),
        prefix = opts.prefix,
        ttl = opts.ttl,
        now = opts.now,
    })
end

return Module
