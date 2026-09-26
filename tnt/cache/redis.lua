--- Драйвер на Redis: кэш один на все узлы и переживает их перезапуск.
---
--- В Redis ходит клиент `tnt-redis`, пришедший аргументом: пул, срок,
--- повторы, TLS и отказ `TntStorageFailure` — его, а кэш от драйвера
--- не зависит и соединений не заводит. Значение — msgpack в строке
--- байтов: Redis хранит строки, а таблица, `int64`, `decimal` доезжают
--- туда и обратно без потерь, как у драйвера `space`.
---
--- **Срок ведёт сам Redis** — мигом `PXAT` у ключа. Просроченное сервер
--- стирает сам, без обхода, и `sweep` здесь нечего делать: обход отдаёт
--- пустой кусок. Живость судит один сервер: `read` отдаёт срок 0 —
--- «живо», — а `add`, `take` и `increment` решают на сервере одной
--- командой. Иначе часы узла и часы Redis, разошедшиеся на миг, давали
--- бы промах у `get` и отказ у `add` на одном и том же ключе.
---
--- **Одной командой — всё, что пишет по прочитанному.** `add` — `SET NX`,
--- `take` — `GETDEL`, `increment` и `erase_if` (снятие своего замка) —
--- сценарии `EVAL`: между чтением и записью здесь сеть, и соседний узел
--- успел бы вклиниться — замок `add` достался бы двоим, счётчик потерял
--- бы прибавку, одноразовый знак `pull` отдался бы дважды, а замок,
--- взятый соседом после срока, снял бы прежний держатель. Отсюда
--- и нижняя граница — Redis 6.2: `PXAT` и `GETDEL` появились в нём.
---
--- **Отказ клиента — бросок самого отказа.** Договор драйвера — бросок,
--- а что из него промах (чтение) и что бросок наружу (запись), решает
--- хранилище; `TntStorageFailure` уходит как есть — с родом
--- и `retriable` для того, кто поймает его на границе запроса.
---
--- **Все ключи — под пространством `namespace`** (`cache:`): `flush`
--- стирает только их — обходом `SCAN` с образцом, а не `FLUSHDB`: база
--- бывает общей с сессиями и счётчиками частоты.
---
--- Драйвер ходит по сети (`remote`), и внутри транзакции box хранилище
--- его не зовёт: уступка на сети порвала бы транзакцию memtx, а запись
--- в Redis не откатилась бы вместе с ней.
---
--- **Тег — упорядоченное множество ключей** под `namespace .. '\0tag:' ..
--- тег`, где счёт ключа — миг его срока в миллисекундах, у вечного —
--- `+inf`. Запись с тегами — один сценарий: `SET` и `ZADD` в каждое
--- множество разом, иначе обрыв между ними оставил бы значение без тега,
--- и забывание тега его не нашло бы. Тот же сценарий вычищает из множества
--- ключи, чей срок вышел, и ставит самому множеству срок последнего
--- ключа: множество не копит мёртвые ключи и уходит вместе с ними.
--- Забывание тега — кусками одного сценария: взять из множества до
--- `limit` ключей, вынуть их оттуда и стереть. Нулевой байт в метке
--- множества — затем, чтобы оно не совпало с ключом приложения: тег
--- `lua` и страница `tag:lua` иначе делили бы один ключ Redis.
---
--- Чего Redis честно не даёт: ключ, стёртый или переписанный без тега,
--- из множества не вынимается — для этого перезапись должна знать прежние
--- теги ключа, то есть читать их лишней командой на каждую запись. Такой
--- ключ ждёт в множестве своего срока, а до него забывание тега сотрёт
--- и его новое значение — лишний промах, а не устаревшее значение.

local ffi = require('ffi')
local msgpack = require('msgpack')

local store = require('tnt.cache.store')

--- Отказ сборки и чужая запись — словом, без места в коде: хранилище
--- кладёт текст в журнал как есть.
local fail = require('tnt.must.fail').raise

local Module = {}

--- Пространство ключей кэша, если не задано иное.
Module.NAMESPACE = 'cache:'

--- Прибавка одним действием сервера.
---
--- KEYS[1] — ключ; ARGV[1] — прибавка; ARGV[2] — миг срока новой записи
--- в миллисекундах эпохи, нет его — навсегда. Число драйвер пишет всегда
--- как msgpack double (`0xcb`), и сценарий читает только его: всё прочее —
--- не число, ответ — пустая строка. Итог пишется тоже double, а не
--- `cmsgpack.pack`: тот пишет 2⁶³ и выше как −2⁶³, а читает uint64 со
--- знаком (проверено на Redis 7.4). У живой записи срок остаётся
--- (`KEEPTTL`), у новой — ставится.
Module.INCREMENT = [[
local payload = redis.call('GET', KEYS[1])
local current = 0
if payload then
    if string.byte(payload) ~= 0xcb then
        return ''
    end
    current = struct.unpack('>d', payload, 2)
end
local packed = struct.pack('>Bd', 0xcb, current + tonumber(ARGV[1]))
redis.call('SET', KEYS[1], packed, 'KEEPTTL')
if not payload and ARGV[2] then
    redis.call('PEXPIREAT', KEYS[1], ARGV[2])
end
return packed
]]

--- Запись с тегами одним действием сервера.
---
--- KEYS[1] — ключ записи, KEYS[2..] — множества её тегов; ARGV[1] — счёт
--- ключа в множествах (миг срока в миллисекундах либо `+inf`), ARGV[2..] —
--- `SET` после ключа: значение, `PXAT`, `NX`. Не легло (`NX` занят) —
--- ответ пустой, и множества не трогаются. Мёртвыми считаются ключи со
--- счётом раньше начала текущей секунды сервера: секунды хватает, а ключ,
--- чей срок ещё идёт, так из множества не выпадет никогда. Счёт последнего
--- ключа — `inf` у вечного: множество тогда тоже вечное.
Module.TAGGED = [[
local stored = redis.call('SET', KEYS[1], unpack(ARGV, 2))
if stored then
    local dead = '(' .. redis.call('TIME')[1] * 1000
    for index = 2, #KEYS do
        redis.call('ZADD', KEYS[index], ARGV[1], KEYS[1])
        redis.call('ZREMRANGEBYSCORE', KEYS[index], '-inf', dead)
        local last = redis.call('ZRANGE', KEYS[index], -1, -1, 'WITHSCORES')[2]
        if last == 'inf' then
            redis.call('PERSIST', KEYS[index])
        elseif last then
            redis.call('PEXPIREAT', KEYS[index], last)
        end
    end
end
return stored
]]

--- Кусок забывания тега одним действием сервера.
---
--- KEYS[1] — множество тега; ARGV[1] — сколько ключей взять. Ответ —
--- сколько ключей взято из множества и сколько из них стёрто: взятых
--- меньше `limit` — тег кончился. Ключи стираются и вынимаются вместе:
--- обрыв между двумя командами оставил бы живое значение без тега.
--- Ключи записей не объявлены в KEYS — их знает только множество;
--- на одном сервере это законно, а Redis Cluster драйвер не держит.
Module.FORGET_TAG = [[
local keys = redis.call('ZRANGE', KEYS[1], 0, ARGV[1] - 1)
if #keys == 0 then
    return { 0, 0 }
end
redis.call('ZREM', KEYS[1], unpack(keys))
return { #keys, redis.call('DEL', unpack(keys)) }
]]

--- Снятие своего замка одним действием сервера: стереть, только если
--- под ключом метка держателя. Из двух команд сосед, взявший замок
--- после срока, лишился бы его между чтением и стиранием.
Module.RELEASE = [[
return redis.call('GET', KEYS[1]) == ARGV[1] and redis.call('DEL', KEYS[1])
]]

--- Метка множества тега в пространстве ключей.
Module.TAG = '\0tag:'

--- Сколько ключей просить у `SCAN` за раз при `flush`.
local CHUNK = 1000

--- Настройки вызова, которые можно повторить после отправки: чтение,
--- запись того же значения (и с тегами), стирание, забывание тега
--- и снятие своего замка. `SET NX`, `GETDEL` и прибавка без них — их
--- повтор решил бы заново то, что сервер уже решил. Повтор снятия замка
--- безвреден: метку держателя не знает никто другой, — но на повторе
--- уже снятый замок ответит «не наш».
local REPEATABLE = { idempotent = true }

--- Под ключом лежит не запись кэша.
local FOREIGN = 'кэш: под ключом в Redis лежит не msgpack — запись не кэша'

---@class TntCacheRedisClient Клиент tnt-redis — то, что драйверу от него нужно
---@field command fun(self: TntCacheRedisClient, args: any[], opts: table|nil): any, any

---@class TntCacheRedisOptions
---@field namespace string|nil Пространство ключей кэша; по умолчанию `cache:`

--- Значение в строку байтов.
---
--- Число — всегда double: сценарий прибавки читает только его, и целое
--- 5, записанное fixint, `increment` иначе не узнал бы. Назад оно
--- приходит тем же числом Lua.
---@param value any
---@return string
local function encoded(value)
    if type(value) == 'number' then
        value = ffi.cast('double', value)
    end

    return msgpack.encode(value)
end

--- Значение из строки байтов. Хвост за значением — чужая запись.
---@param payload string
---@return any
local function decoded(payload)
    local value, position = msgpack.decode(payload)

    if position ~= #payload + 1 then
        fail(FOREIGN)
    end

    return value
end

--- Миг срока в миллисекундах эпохи — вверх: сервер не сотрёт раньше срока.
---@param expires_at number
---@return integer
local function milliseconds(expires_at)
    return math.ceil(expires_at * 1000)
end

--- Команда записи с мигом срока, если он есть.
---@param args any[]
---@param expires_at number
---@return any[]
local function lasting(args, expires_at)
    if expires_at ~= store.FOREVER then
        table.insert(args, 'PXAT')
        table.insert(args, milliseconds(expires_at))
    end

    return args
end

--- Заводит драйвер над клиентом tnt-redis.
---@param client TntCacheRedisClient|nil Нет его — ошибка программиста
---@param opts TntCacheRedisOptions|nil
---@return TntCacheDriver
function Module.new(client, opts)
    if type(client) ~= 'table' or type(client.command) ~= 'function' then
        fail('кэш: драйверу redis нужен клиент tnt-redis — аргумент redis')
    end

    local namespace = (opts or {}).namespace or Module.NAMESPACE

    if type(namespace) ~= 'string' or namespace == '' then
        fail(
            'кэш: namespace драйвера redis — непустая строка: без неё flush стёр бы чужие ключи базы'
        )
    end

    -- Образец SCAN — глоб: знаки глоба в пространстве берутся буквально.
    local pattern = namespace:gsub('[%*%?%[%]\\]', '\\%0') .. '*'

    --- Ответ сервера; отказ клиента — бросок самого отказа.
    ---@param args any[]
    ---@param call table|nil Настройки вызова
    ---@return any
    local function send(args, call)
        local value, err = client:command(args, call)

        if err ~= nil then
            error(err)
        end

        return value
    end

    --- Стирает ключи сервера одной командой.
    ---@param keys string[] Ключи вместе с пространством
    local function delete(keys)
        local args = { 'DEL' }

        for index, key in ipairs(keys) do
            args[index + 1] = key
        end

        send(args, REPEATABLE)
    end

    --- Запись с тегами сценарием: ключ, множества тегов, счёт ключа
    --- и остаток `SET` после ключа.
    ---@param key string
    ---@param value any
    ---@param expires_at number
    ---@param tags string[]
    ---@param mode string[] `{ 'NX' }` либо пусто
    ---@return any[]
    local function tagged(key, value, expires_at, tags, mode)
        local args = { 'EVAL', Module.TAGGED, #tags + 1, namespace .. key }

        ---@type string|integer
        local score = '+inf'

        for _, tag in ipairs(tags) do
            table.insert(args, namespace .. Module.TAG .. tag)
        end

        if expires_at ~= store.FOREVER then
            score = milliseconds(expires_at)
        end

        table.insert(args, score)

        for _, part in ipairs(lasting({ encoded(value) }, expires_at)) do
            table.insert(args, part)
        end

        for _, part in ipairs(mode) do
            table.insert(args, part)
        end

        return args
    end

    --- Значение из ответа чтения: промах — пусто, иначе живо.
    ---@param payload string|nil
    ---@return any value
    ---@return number|nil expires_at
    local function found(payload)
        if payload == nil then
            return nil
        end

        return decoded(payload), store.FOREVER
    end

    return {
        name = 'redis',
        remote = true,
        read = function(key)
            return found(send({ 'GET', namespace .. key }, REPEATABLE))
        end,
        take = function(key)
            return found(send({ 'GETDEL', namespace .. key }))
        end,
        write = function(key, value, expires_at, tags)
            if tags == nil then
                send(lasting({ 'SET', namespace .. key, encoded(value) }, expires_at), REPEATABLE)
            else
                send(tagged(key, value, expires_at, tags, {}), REPEATABLE)
            end
        end,
        add = function(key, value, expires_at, tags)
            if tags ~= nil then
                return send(tagged(key, value, expires_at, tags, { 'NX' })) ~= nil
            end

            return send(lasting({ 'SET', namespace .. key, encoded(value), 'NX' }, expires_at)) ~= nil
        end,
        erase_if = function(key, value)
            return send({ 'EVAL', Module.RELEASE, 1, namespace .. key, encoded(value) }, REPEATABLE) == 1
        end,
        erase_tagged = function(tag, limit)
            local reply = send({ 'EVAL', Module.FORGET_TAG, 1, namespace .. Module.TAG .. tag, limit }, REPEATABLE)

            return reply[1], reply[2]
        end,
        increment = function(key, by, expires_at)
            local args = { 'EVAL', Module.INCREMENT, 1, namespace .. key, by }

            if expires_at ~= store.FOREVER then
                table.insert(args, milliseconds(expires_at))
            end

            local packed = send(args)

            -- Пустой ответ — под ключом не число.
            if packed == '' then
                return nil
            end

            return (msgpack.decode(packed))
        end,
        erase = function(key)
            send({ 'DEL', namespace .. key }, REPEATABLE)
        end,
        erase_many = function(keys)
            local full = {}

            for index, key in ipairs(keys) do
                full[index] = namespace .. key
            end

            delete(full)
        end,
        clear = function()
            local cursor = '0'

            -- SCAN не снимок: ключ, дописанный за обходом, может уцелеть,
            -- а отданный дважды стирается дважды — DEL пропавшего ключа
            -- не отказ.
            repeat
                local reply = send({ 'SCAN', cursor, 'MATCH', pattern, 'COUNT', CHUNK }, REPEATABLE)
                local keys = reply[2]

                cursor = reply[1]

                if #keys > 0 then
                    delete(keys)
                end
            until cursor == '0'
        end,
        scan = function()
            -- Просроченное Redis стирает сам по мигу PXAT: обходить нечего.
            return {}, nil
        end,
    }
end

return Module
