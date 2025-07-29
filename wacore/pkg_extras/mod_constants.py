"""Constants used by Home Assistant components."""
from __future__ import annotations

from typing import Final

MAJOR_VERSION: Final = 2022
MINOR_VERSION: Final = 2
PATCH_VERSION: Final = "0.dev0"
__short_version__: Final = f"{MAJOR_VERSION}.{MINOR_VERSION}"
__version__: Final = f"{__short_version__}.{PATCH_VERSION}"


# Format for platform files
PLATFORM_FORMAT: Final = "{platform}.{domain}"

# Catch all when registering state or event listeners.
MATCH_ALL: Final = "*"

# If no name is specified
ANY_DEFAULT_NAME: Final = "Unnamed Name"

# Max characters for data stored in the recorder 
MAX_LENGTH_EVENT_EVENT_TYPE: Final = 64
MAX_LENGTH_EVENT_ORIGIN: Final = 32
MAX_LENGTH_EVENT_CONTEXT_ID: Final = 36
MAX_LENGTH_STATE_DOMAIN: Final = 64
MAX_LENGTH_STATE_ENTITY_ID: Final = 255
MAX_LENGTH_STATE_STATE: Final = 255


# #### CONFIG ####
CONF_ACCESS_TOKEN: Final = "access_token"
CONF_ADDRESS: Final = "address"
CONF_ALIAS: Final = "alias"
CONF_ALLOWLIST_EXTERNAL_URLS: Final = "allowlist_external_urls"
CONF_API_KEY: Final = "api_key"
CONF_API_TOKEN: Final = "api_token"
CONF_API_VERSION: Final = "api_version"

# #### EVENTS ####
EVENT_CALL_SERVICE: Final = "call_service"
EVENT_COMPONENT_LOADED: Final = "component_loaded"
EVENT_SERVICE_REGISTERED: Final = "service_registered"
EVENT_SERVICE_REMOVED: Final = "service_removed"
EVENT_STATE_CHANGED: Final = "state_changed"
EVENT_THEMES_UPDATED: Final = "themes_updated"
EVENT_TIMER_OUT_OF_SYNC: Final = "timer_out_of_sync"
EVENT_TIME_CHANGED: Final = "time_changed"

# #### STATES ####
STATE_START: Final = "start"
STATE_STOP: Final = "stop"
STATE_HOME: Final = "home"
STATE_NOT_HOME: Final = "not_home"
STATE_UNKNOWN: Final = "unknown"
STATE_OPEN: Final = "open"
STATE_OPENING: Final = "opening"
STATE_CLOSED: Final = "closed"
STATE_CLOSING: Final = "closing"
STATE_IDLE: Final = "idle"
STATE_STANDBY: Final = "standby"
STATE_LOCKED: Final = "locked"
STATE_UNLOCKED: Final = "unlocked"
STATE_LOCKING: Final = "locking"
STATE_UNLOCKING: Final = "unlocking"
STATE_JAMMED: Final = "jammed"
STATE_UNAVAILABLE: Final = "unavailable"
STATE_OK: Final = "ok"
STATE_PROBLEM: Final = "problem"

# Contains time-related attributes
ATTR_NOW: Final = "now"
ATTR_DATE: Final = "date"
ATTR_TIME: Final = "time"
ATTR_SECONDS: Final = "seconds"

# Contains domain, service for a SERVICE_CALL event
ATTR_DOMAIN: Final = "domain"
ATTR_SERVICE: Final = "service"
ATTR_SERVICE_DATA: Final = "service_data"

# IDs
ATTR_ID: Final = "id"

# Name
ATTR_NAME: Final = "name"

# Currency units
CURRENCY_DOLLAR: Final = "$"

# Time units
TIME_MICROSECONDS: Final = "μs"
TIME_MILLISECONDS: Final = "ms"
TIME_SECONDS: Final = "s"
TIME_MINUTES: Final = "min"
TIME_HOURS: Final = "h"
TIME_DAYS: Final = "d"
TIME_WEEKS: Final = "w"
TIME_MONTHS: Final = "m"
TIME_YEARS: Final = "y"

# Data units
DATA_BITS: Final = "bit"
DATA_KILOBITS: Final = "kbit"
DATA_MEGABITS: Final = "Mbit"
DATA_GIGABITS: Final = "Gbit"
DATA_BYTES: Final = "B"
DATA_KILOBYTES: Final = "kB"
DATA_MEGABYTES: Final = "MB"
DATA_GIGABYTES: Final = "GB"
DATA_TERABYTES: Final = "TB"
DATA_PETABYTES: Final = "PB"
DATA_EXABYTES: Final = "EB"
DATA_ZETTABYTES: Final = "ZB"
DATA_YOTTABYTES: Final = "YB"
DATA_KIBIBYTES: Final = "KiB"
DATA_MEBIBYTES: Final = "MiB"
DATA_GIBIBYTES: Final = "GiB"
DATA_TEBIBYTES: Final = "TiB"
DATA_PEBIBYTES: Final = "PiB"
DATA_EXBIBYTES: Final = "EiB"
DATA_ZEBIBYTES: Final = "ZiB"
DATA_YOBIBYTES: Final = "YiB"

# Data_rate units
DATA_RATE_BITS_PER_SECOND: Final = "bit/s"
DATA_RATE_KILOBITS_PER_SECOND: Final = "kbit/s"
DATA_RATE_MEGABITS_PER_SECOND: Final = "Mbit/s"
DATA_RATE_GIGABITS_PER_SECOND: Final = "Gbit/s"
DATA_RATE_BYTES_PER_SECOND: Final = "B/s"
DATA_RATE_KILOBYTES_PER_SECOND: Final = "kB/s"
DATA_RATE_MEGABYTES_PER_SECOND: Final = "MB/s"
DATA_RATE_GIGABYTES_PER_SECOND: Final = "GB/s"
DATA_RATE_KIBIBYTES_PER_SECOND: Final = "KiB/s"
DATA_RATE_MEBIBYTES_PER_SECOND: Final = "MiB/s"
DATA_RATE_GIBIBYTES_PER_SECOND: Final = "GiB/s"

# #### API / REMOTE /PORT ####
SERVER_PORT: Final = 8123

URL_ROOT: Final = "/"
URL_API: Final = "/api/"
URL_API_STREAM: Final = "/api/stream"
URL_API_CONFIG: Final = "/api/config"
URL_API_STATES: Final = "/api/states"
URL_API_STATES_ENTITY: Final = "/api/states/{}"
URL_API_EVENTS: Final = "/api/events"
URL_API_EVENTS_EVENT: Final = "/api/events/{}"
URL_API_SERVICES: Final = "/api/services"
URL_API_SERVICES_SERVICE: Final = "/api/services/{}/{}"
URL_API_COMPONENTS: Final = "/api/components"
URL_API_ERROR_LOG: Final = "/api/error_log"
URL_API_LOG_OUT: Final = "/api/log_out"
URL_API_TEMPLATE: Final = "/api/template"

HTTP_BASIC_AUTHENTICATION: Final = "basic"
HTTP_BEARER_AUTHENTICATION: Final = "bearer_token"
HTTP_DIGEST_AUTHENTICATION: Final = "digest"

HTTP_HEADER_X_REQUESTED_WITH: Final = "X-Requested-With"

CONTENT_TYPE_JSON: Final = "application/json"
CONTENT_TYPE_MULTIPART: Final = "multipart/x-mixed-replace; boundary={}"
CONTENT_TYPE_TEXT_PLAIN: Final = "text/plain"

# The exit code to send to request a restart
EXIT_CODE: Final = 100

UNIT_NOT_RECOGNIZED_TEMPLATE: Final = "{} is not a recognized {} unit."
