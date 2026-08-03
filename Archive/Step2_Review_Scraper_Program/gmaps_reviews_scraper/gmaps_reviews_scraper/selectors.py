"""Selector registry for Google Maps DOM interactions.

Google Maps changes markup over time. Every action in the scraper uses selector
fallback lists from this module, so selector maintenance is centralized and easy.
"""

# Business landing checks.
BUSINESS_TITLE_SELECTORS = [
    "h1.DUwDvf",
    "h1.fontHeadlineLarge",
    "div[role='main'] h1",
]

# Search results panel entries when query does not auto-open a place page.
SEARCH_RESULT_LINK_SELECTORS = [
    "button.hfpxzc",
    "div[role='article'] button.hfpxzc",
    "div.Nv2PK button.hfpxzc",
    "a.hfpxzc",
    "div[role='article'] a.hfpxzc",
    "div[role='feed'] a.hfpxzc",
    "div.Nv2PK a.hfpxzc",
    "div.Nv2PK a",
    "a[href*='/maps/place/']",
]

# Consent dialogs.
COOKIE_ACCEPT_SELECTORS = [
    "button[aria-label*='Accept all']",
    "button:has-text('Accept all')",
    "button:has-text('I agree')",
    "button:has-text('Accept')",
]

# Reviews tab triggers.
REVIEWS_TAB_SELECTORS = [
    "button[aria-label*='Reviews']",
    "button[aria-label*='reviews']",
    "button[role='tab']:has-text('Reviews')",
    "div[role='tab']:has-text('Reviews')",
    "button.hh2c6:has-text('Reviews')",
]

# Fallback entry points that often open review overlays/panels in limited or
# variant UIs where an explicit "Reviews" tab is absent.
REVIEWS_ENTRYPOINT_SELECTORS = [
    "button[aria-label*='stars'][aria-label*='Reviews']",
    "button[aria-label*='stars'][aria-label*='reviews']",
    "span[role='img'][aria-label*='stars'][aria-label*='Reviews']",
    "span[role='img'][aria-label*='stars'][aria-label*='reviews']",
    "button[aria-label*='Write a review']",
    "button[data-value='Write a review']",
]

# Sort dialog.
SORT_BUTTON_SELECTORS = [
    "button[aria-label*='Sort reviews']",
    "button[data-value='Sort']",
    "button.g88MCb",
]

NEWEST_OPTION_SELECTORS = [
    "div[role='menuitemradio']:has-text('Newest')",
    "div[role='menuitem']:has-text('Newest')",
    "li[data-index='1']",
]

# Review list container and cards.
REVIEW_CARD_SELECTORS = [
    "div.jftiEf",
    "div[data-review-id]",
]

REVIEW_SCROLL_CONTAINER_SELECTORS = [
    "div.m6QErb.DxyBCb",
    "div.m6QErb.DByNcb",
    "div.m6QErb",
]

# Individual review fields.
REVIEW_SEE_MORE_SELECTORS = [
    "button[aria-label*='See more']",
    "button.w8nwRe",
]

REVIEWER_NAME_SELECTORS = [
    "div.d4r55",
    "div.WNxzHc span",
]

REVIEW_RATING_SELECTORS = [
    "span.kvMYJc",
    "span[role='img'][aria-label*='star']",
]

REVIEW_DATE_SELECTORS = [
    "span.rsqaWe",
]

REVIEW_TEXT_SELECTORS = [
    "span.wiI7pd",
    "span.MyEned",
]

REVIEW_STATS_SELECTORS = [
    "div.RfnDt",
]

REVIEW_HELPFUL_BUTTON_SELECTORS = [
    "button[aria-label*='helpful']",
]

REVIEW_PHOTO_SELECTORS = [
    "div.Iop04 img",
    "img.NBa7we",
]

REVIEW_OWNER_RESPONSE_SELECTORS = [
    "text='Response from the owner'",
]

REVIEW_SERVICE_TYPE_SELECTORS = [
    "div.PBK6be span.pVtsbf",
]

# Business metadata extraction.
BUSINESS_NAME_SELECTORS = [
    "h1.DUwDvf",
    "h1.fontHeadlineLarge",
    "div[role='main'] h1",
    "h1",
]

BUSINESS_RATING_SELECTORS = [
    "div.F7nice span[aria-hidden='true']",
    "span.ceNzKf",
]

BUSINESS_REVIEWS_BUTTON_SELECTORS = [
    "button[aria-label*='reviews']",
]

BUSINESS_REVIEWS_COUNT_TEXT_SELECTORS = [
    "div.F7nice span",
    "span.UY7F9",
]

BUSINESS_ADDRESS_SELECTORS = [
    "button[data-item-id='address']",
]

BUSINESS_PHONE_SELECTORS = [
    "button[data-item-id*='phone:tel']",
]

BUSINESS_WEBSITE_SELECTORS = [
    "a[data-item-id='authority']",
]

BUSINESS_CATEGORY_SELECTORS = [
    "button.DkEaL",
    "div.DkEaL",
]

# Closed-state checks.
CLOSED_TOKENS = [
    "Permanently closed",
    "Temporarily closed",
]
