from enum import Enum, StrEnum, unique


class TimeSeriesMode(StrEnum):
    VOLUME = "timelinevol"
    VOLUME_RAW = "timelinevolraw"
    TONE = "timelinetone"
    LANGUAGE = "timelinelang"
    SOURCE_COUNTRY = "timelinesourcecountry"


class ArticleMode(StrEnum):
    ARTICLE_LIST = "artlist"


Mode = TimeSeriesMode | ArticleMode


@unique
class HttpResponseCodes(Enum):
    OK = 200
    BAD_REQUEST = 400
    NOT_FOUND = 404
    RATE_LIMIT = 429
