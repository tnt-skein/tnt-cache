--- Замок на ключ и `remember`, который считает значение один раз.
---
--- **Замок — запись кэша со сроком и меткой держателя.**
--- `store:lock(key, ttl)` берёт его через `add`, а `release` снимает
--- только свой: метка — случайный UUID, и держатель, чей срок вышел,
--- замка соседа не снимет. Срок обязателен — замок, брошенный упавшим
--- держателем, иначе не освободился бы никогда. Ждать замка — `wait`:
--- ожидающий спрашивает снова раз в `POLL` секунд и не дольше срока.
--- Замок лежит под `<приставка>\0lock:<ключ>` — нулевой байт не встречается
--- в ключах приложения, и замок не спутать со значением того же ключа.
---
--- **`remember` с замком считает значение один раз**, остальные ждут:
--- промах берёт замок ключа, взявший считает и кладёт, а прочие
--- спрашивают кэш, пока значение не появится, и берут замок сами, если
--- держатель ушёл без значения. Взявший читает кэш ещё раз: сосед мог
--- досчитать между его промахом и замком, и значение иначе считалось бы
--- дважды.
---
---     local page, err = store:remember('docs:index', 300, render, { lock = 30 })
---
--- Хранилище приходит сюда аргументом: `Store:lock` и `Store:remember` —
--- тонкие входы, а запись через драйвер замок берёт у `tnt.cache.record`.

local record = require('tnt.cache.record')

--- Бросок без места в коде: у отказа настройки места нет, у чужой
--- поломки, брошенной заново, оно уже есть. Значение идёт как есть —
--- чужая поломка остаётся тем, чем её бросили.
local fail = require('tnt.must.fail').raise

---@class TntCacheLockOptions
---@field wait number|nil Сколько секунд ждать занятого замка; по умолчанию 0 — не ждать

---@class TntCacheRememberOptions
---@field lock number Срок замка на время счёта, секунды
---@field wait number|nil Сколько секунд ждать чужого счёта; по умолчанию — срок замка

---@class TntCacheLock Взятый замок: снимает его только взявший
---@field store TntCacheStore
---@field key string Ключ, названный при взятии
---@field full string Ключ замка у драйвера
---@field token string Метка держателя
local Lock = {}
Lock.__index = Lock

--- Метка замка между приставкой и ключом.
local LOCK = '\0lock:'

--- Как часто ожидающий замка спрашивает снова, секунды.
---
--- Опрос, а не побудка: держатель бывает на другом узле, и сказать
--- ожидающему о снятии ему нечем. Двадцать раз в секунду — задержка
--- ожидающего не больше 50 мс и двадцать коротких обращений к драйверу
--- в секунду на ожидающего.
local POLL = 0.05

--- Тексты отказов замка.
local BAD_LOCK = 'кэш: срок замка — конечное число секунд больше нуля: '
    .. 'замок без срока, брошенный упавшим держателем, не освободится никогда'
local BAD_WAIT = 'кэш: wait — конечное число секунд не меньше нуля'
local BAD_REMEMBER = 'кэш: настройки remember — таблица { lock, wait }'
local BAD_LOCK_OPTIONS = 'кэш: настройки lock должны быть таблицей'
local WAIT_IN_TRANSACTION =
    'кэш: ожидание замка уступает управление — внутри транзакции ждут только с wait = 0'
local BUSY = 'кэш: замок %s занят'
local UNWAITED = 'кэш: %s считает сосед — значения не дождались за %s с'

local Module = { POLL = POLL }

--- Внешние средства — общие с хранилищем: подмена в проверке одна.
local source = record.source

--- Ключ замка с приставкой: метка отделяет его от значения того же ключа.
---@param store TntCacheStore
---@param key any
---@return string
local function locked(store, key)
    return store.prefix .. LOCK .. record.checked(store, key)
end

--- Срок замка: конечное число секунд больше нуля.
---
--- Сравнение записано отрицанием нарочно: NaN не больше нуля и не меньше,
--- и прямое «меньше либо равно нулю» пропустило бы его — замок со сроком
--- NaN в памяти не истёк бы никогда.
---@param value any
---@return number
local function lock_ttl_of(value)
    if type(value) ~= 'number' or not (value > 0 and value < math.huge) then
        fail(BAD_LOCK)
    end

    return value
end

--- Сколько ждать замка: конечное число секунд не меньше нуля.
---@param value any
---@return number
local function wait_of(value)
    if type(value) ~= 'number' or not (value >= 0 and value < math.huge) then
        fail(BAD_WAIT)
    end

    return value
end

--- Ожидание внутри транзакции — ошибка программиста: пауза уступает
--- управление, и транзакция memtx оборвалась бы молча, до фиксации.
--- Без ожидания замок берут и в транзакции: взятие — одна запись.
---@param wait number
---@return number
local function patient(wait)
    if wait > 0 and source().in_transaction() then
        fail(WAIT_IN_TRANSACTION)
    end

    return wait
end

--- Одна попытка взять замок.
---@param store TntCacheStore
---@param key string
---@param seconds number Срок замка
---@return TntCacheLock|nil
local function seized(store, key, seconds)
    local full = locked(store, key)
    local token = source().token()

    if not record.absent(store, full, token, seconds, nil) then
        return nil
    end

    return setmetatable({ store = store, key = key, full = full, token = token }, Lock)
end

--- Вышел ли срок ожидания; не вышел — пауза перед следующей попыткой.
---
--- Срок отмерен настоящими часами, а остаток сна — от отметки цикла
--- событий, от которой считает и сам сон: так ожидание кончается ровно
--- в срок, была ли перед ним работа без уступки или нет.
---@param deadline number Миг конца ожидания по монотонным часам
---@return boolean over
local function over(deadline)
    local done = source().monotonic() >= deadline

    if not done then
        source().sleep(math.min(POLL, deadline - source().scheduler_now()))
    end

    return done
end

--- Берёт замок на ключ — тело `Store:lock`.
---
--- Занят и за `wait` не освободился — `nil, err`: так бывает, и решать,
--- ждать ли дольше, вызывающему. Отказ драйвера бросает, как у `add`:
--- промах здесь означал бы «замок свободен».
---@param store TntCacheStore
---@param key string
---@param ttl number Срок замка, секунды
---@param opts TntCacheLockOptions|nil
---@return TntCacheLock|nil lock
---@return string|nil err
local function take(store, key, ttl, opts)
    local seconds = lock_ttl_of(ttl)

    opts = opts or {}

    if type(opts) ~= 'table' then
        fail(BAD_LOCK_OPTIONS)
    end

    local deadline = source().monotonic() + patient(wait_of(opts.wait or 0))
    local lock = seized(store, key, seconds)

    while lock == nil do
        if over(deadline) then
            return nil, BUSY:format(key)
        end

        lock = seized(store, key, seconds)
    end

    return lock
end

--- Снимает замок, если он всё ещё свой.
---
--- `false` — замок уже не наш: срок вышел, и его стёр либо взял другой.
--- Отказ драйвера бросает, как всякая запись: замок, который не удалось
--- снять, ждёт своего срока, и вызывающему стоит об этом знать.
---@return boolean released
function Lock:release()
    local store = self.store
    local driver = store.driver

    record.outside_transaction(store)

    if driver.erase_if ~= nil then
        return driver.erase_if(self.full, self.token)
    end

    local value, found = record.alive(store, self.full, driver.read)

    if not found or value ~= self.token then
        return false
    end

    driver.erase(self.full)

    return true
end

--- Считает под взятым замком и снимает его, чем бы счёт ни кончился.
---
--- Кэш читается ещё раз уже под замком: сосед мог досчитать и снять
--- замок между нашим промахом и взятием, и без второго чтения значение
--- считалось бы дважды. Это чтение — часть того же обращения, и в ряды
--- метрик оно не идёт: обращение уже посчитано промахом. Поломка счёта
--- уходит как есть, но после снятия замка: иначе ожидающие ждали бы его
--- срока.
---@param store TntCacheStore
---@param lock TntCacheLock
---@param key string
---@param ttl number|nil
---@param compute fun(): any
---@param tags string[]|nil
---@return any
local function filled(store, lock, key, ttl, compute, tags)
    local ok, value = pcall(record.computed, store, key, ttl, compute, tags, record.fetched)

    lock:release()

    if not ok then
        fail(value)
    end

    return value
end

--- Значение, посчитанное одним: промах берёт замок, остальные ждут.
---@param store TntCacheStore
---@param key string
---@param ttl number|nil
---@param compute fun(): any
---@param tags string[]|nil
---@param opts any Настройки замка
---@return any value
---@return string|nil err
local function shared(store, key, ttl, compute, tags, opts)
    if type(opts) ~= 'table' then
        fail(BAD_REMEMBER)
    end

    local seconds = lock_ttl_of(opts.lock)
    local wait = patient(wait_of(opts.wait or seconds))
    local full = record.keyed(store, key)
    local value, found = record.asked(store, full, store.driver.read)

    if found then
        return value
    end

    local deadline = source().monotonic() + wait
    local lock = seized(store, key, seconds)

    -- Ждущий спрашивает значение, а не только замок: держатель, снявший
    -- замок со значением, отпускает всех, и никто не берёт замок зря.
    -- Опрос мимо рядов: двадцать промахов в секунду на ожидающего
    -- говорили бы о замке, а не о кэше.
    while lock == nil do
        if over(deadline) then
            return nil, UNWAITED:format(key, wait)
        end

        value, found = record.fetched(store, full, store.driver.read)

        if found then
            return value
        end

        lock = seized(store, key, seconds)
    end

    ---@cast lock TntCacheLock

    return filled(store, lock, key, ttl, compute, tags)
end

--- Запоминает значение с тегами либо без — тело `remember` хранилища
--- и вида с тегами; с настройками замка считает одним на всех.
---
--- Без настроек замок не берётся вовсе: промах считает сам, как всякий
--- `remember`, и развилка стоит здесь, чтобы у хранилища и вида она
--- была одна.
---@param store TntCacheStore
---@param key string
---@param ttl number|nil
---@param compute fun(): any
---@param tags string[]|nil
---@param opts TntCacheRememberOptions|nil
---@return any value
---@return string|nil err
local function remember(store, key, ttl, compute, tags, opts)
    if opts == nil then
        return record.computed(store, key, ttl, compute, tags, record.asked)
    end

    return shared(store, key, ttl, compute, tags, opts)
end

Module.take = take
Module.remember = remember

return Module
