#![allow(dead_code)]
//! Committed Telegram Bot API method metadata.
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
pub const BOT_API_VERSION: &str = "10.3";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParamType {
    String,
    Integer,
    Float,
    Boolean,
    Object,
    Array,
    InputFile,
    StringOrInteger,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MethodFamily {
    Send,
    Copy,
    Forward,
    Edit,
    Delete,
    Admin,
    Sticker,
    Payment,
    Other,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuccessProjection {
    Message,
    FirstMessage,
    None,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParamSpec {
    pub name: &'static str,
    pub kind: ParamType,
    pub required: bool,
    pub file: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MethodSpec {
    pub name: &'static str,
    pub family: MethodFamily,
    pub projection: SuccessProjection,
    pub params: &'static [ParamSpec],
}

const CHAT: ParamSpec = ParamSpec {
    name: "chat_id",
    kind: ParamType::StringOrInteger,
    required: true,
    file: false,
};
const USER: ParamSpec = ParamSpec {
    name: "user_id",
    kind: ParamType::Integer,
    required: true,
    file: false,
};
const TEXT: ParamSpec = ParamSpec {
    name: "text",
    kind: ParamType::String,
    required: true,
    file: false,
};
const MSG: ParamSpec = ParamSpec {
    name: "message_id",
    kind: ParamType::Integer,
    required: true,
    file: false,
};
const FROM_CHAT: ParamSpec = ParamSpec {
    name: "from_chat_id",
    kind: ParamType::StringOrInteger,
    required: true,
    file: false,
};
const FILE: ParamSpec = ParamSpec {
    name: "media",
    kind: ParamType::InputFile,
    required: true,
    file: true,
};
const PHOTO: ParamSpec = ParamSpec {
    name: "photo",
    kind: ParamType::InputFile,
    required: true,
    file: true,
};
const DOC: ParamSpec = ParamSpec {
    name: "document",
    kind: ParamType::InputFile,
    required: true,
    file: true,
};
const AUDIO: ParamSpec = ParamSpec {
    name: "audio",
    kind: ParamType::InputFile,
    required: true,
    file: true,
};
const VIDEO: ParamSpec = ParamSpec {
    name: "video",
    kind: ParamType::InputFile,
    required: true,
    file: true,
};
const ANIMATION: ParamSpec = ParamSpec {
    name: "animation",
    kind: ParamType::InputFile,
    required: true,
    file: true,
};
const VOICE: ParamSpec = ParamSpec {
    name: "voice",
    kind: ParamType::InputFile,
    required: true,
    file: true,
};
const NOTE: ParamSpec = ParamSpec {
    name: "video_note",
    kind: ParamType::InputFile,
    required: true,
    file: true,
};
const STICKER: ParamSpec = ParamSpec {
    name: "sticker",
    kind: ParamType::InputFile,
    required: true,
    file: true,
};
const LAT: ParamSpec = ParamSpec {
    name: "latitude",
    kind: ParamType::Float,
    required: true,
    file: false,
};
const LON: ParamSpec = ParamSpec {
    name: "longitude",
    kind: ParamType::Float,
    required: true,
    file: false,
};
const QUESTION: ParamSpec = ParamSpec {
    name: "question",
    kind: ParamType::String,
    required: true,
    file: false,
};
const OPTIONS: ParamSpec = ParamSpec {
    name: "options",
    kind: ParamType::Array,
    required: true,
    file: false,
};
const PHONE: ParamSpec = ParamSpec {
    name: "phone_number",
    kind: ParamType::String,
    required: true,
    file: false,
};
const FIRST: ParamSpec = ParamSpec {
    name: "first_name",
    kind: ParamType::String,
    required: true,
    file: false,
};
const MEDIA: ParamSpec = ParamSpec {
    name: "media",
    kind: ParamType::Array,
    required: true,
    file: true,
};
const ACTION: ParamSpec = ParamSpec {
    name: "action",
    kind: ParamType::String,
    required: true,
    file: false,
};
const NAME: ParamSpec = ParamSpec {
    name: "name",
    kind: ParamType::String,
    required: true,
    file: false,
};
const TITLE: ParamSpec = ParamSpec {
    name: "title",
    kind: ParamType::String,
    required: true,
    file: false,
};
const PARSE_MODE: ParamSpec = ParamSpec {
    name: "parse_mode",
    kind: ParamType::String,
    required: false,
    file: false,
};
const ENTITIES: ParamSpec = ParamSpec {
    name: "entities",
    kind: ParamType::Array,
    required: false,
    file: false,
};
const LINK_PREVIEW_OPTS: ParamSpec = ParamSpec {
    name: "link_preview_options",
    kind: ParamType::Object,
    required: false,
    file: false,
};
const DISABLE_NOTIF: ParamSpec = ParamSpec {
    name: "disable_notification",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const PROTECT_CONTENT: ParamSpec = ParamSpec {
    name: "protect_content",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const REPLY_PARAMS: ParamSpec = ParamSpec {
    name: "reply_parameters",
    kind: ParamType::Object,
    required: false,
    file: false,
};
const REPLY_MARKUP: ParamSpec = ParamSpec {
    name: "reply_markup",
    kind: ParamType::Object,
    required: false,
    file: false,
};
const ALLOW_PAID: ParamSpec = ParamSpec {
    name: "allow_paid_broadcast",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const MESSAGE_THREAD_ID: ParamSpec = ParamSpec {
    name: "message_thread_id",
    kind: ParamType::Integer,
    required: false,
    file: false,
};
const BUSINESS_CONN_ID: ParamSpec = ParamSpec {
    name: "business_connection_id",
    kind: ParamType::String,
    required: false,
    file: false,
};
const CAPTION: ParamSpec = ParamSpec {
    name: "caption",
    kind: ParamType::String,
    required: false,
    file: false,
};
const HAS_SPOILER: ParamSpec = ParamSpec {
    name: "has_spoiler",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const DURATION: ParamSpec = ParamSpec {
    name: "duration",
    kind: ParamType::Integer,
    required: false,
    file: false,
};
const PERFORMER: ParamSpec = ParamSpec {
    name: "performer",
    kind: ParamType::String,
    required: false,
    file: false,
};
const WIDTH: ParamSpec = ParamSpec {
    name: "width",
    kind: ParamType::Integer,
    required: false,
    file: false,
};
const HEIGHT: ParamSpec = ParamSpec {
    name: "height",
    kind: ParamType::Integer,
    required: false,
    file: false,
};
const THUMBNAIL: ParamSpec = ParamSpec {
    name: "thumbnail",
    kind: ParamType::InputFile,
    required: false,
    file: true,
};
const LIVE_PERIOD: ParamSpec = ParamSpec {
    name: "live_period",
    kind: ParamType::Integer,
    required: false,
    file: false,
};
const HORIZONTAL_ACCURACY: ParamSpec = ParamSpec {
    name: "horizontal_accuracy",
    kind: ParamType::Float,
    required: false,
    file: false,
};
const HEADING: ParamSpec = ParamSpec {
    name: "heading",
    kind: ParamType::Integer,
    required: false,
    file: false,
};
const PROXIMITY_ALERT_RADIUS: ParamSpec = ParamSpec {
    name: "proximity_alert_radius",
    kind: ParamType::Integer,
    required: false,
    file: false,
};
const VIDEO_START_TS: ParamSpec = ParamSpec {
    name: "video_start_timestamp",
    kind: ParamType::Integer,
    required: false,
    file: false,
};
const EMOJI: ParamSpec = ParamSpec {
    name: "emoji",
    kind: ParamType::String,
    required: false,
    file: false,
};
const QUESTION_PARSE_MODE: ParamSpec = ParamSpec {
    name: "question_parse_mode",
    kind: ParamType::String,
    required: false,
    file: false,
};
const QUESTION_ENTITIES: ParamSpec = ParamSpec {
    name: "question_entities",
    kind: ParamType::Array,
    required: false,
    file: false,
};
const IS_ANONYMOUS: ParamSpec = ParamSpec {
    name: "is_anonymous",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const TYPE: ParamSpec = ParamSpec {
    name: "type",
    kind: ParamType::String,
    required: false,
    file: false,
};
const ALLOWS_MULTIPLE_ANSWERS: ParamSpec = ParamSpec {
    name: "allows_multiple_answers",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const CORRECT_OPTION_ID: ParamSpec = ParamSpec {
    name: "correct_option_id",
    kind: ParamType::Integer,
    required: false,
    file: false,
};
const EXPLANATION: ParamSpec = ParamSpec {
    name: "explanation",
    kind: ParamType::String,
    required: false,
    file: false,
};
const EXPLANATION_PARSE_MODE: ParamSpec = ParamSpec {
    name: "explanation_parse_mode",
    kind: ParamType::String,
    required: false,
    file: false,
};
const EXPLANATION_ENTITIES: ParamSpec = ParamSpec {
    name: "explanation_entities",
    kind: ParamType::Array,
    required: false,
    file: false,
};
const OPEN_PERIOD: ParamSpec = ParamSpec {
    name: "open_period",
    kind: ParamType::Integer,
    required: false,
    file: false,
};
const CLOSE_DATE: ParamSpec = ParamSpec {
    name: "close_date",
    kind: ParamType::Integer,
    required: false,
    file: false,
};
const IS_CLOSED: ParamSpec = ParamSpec {
    name: "is_closed",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const EMOJI_PAYLOAD: ParamSpec = ParamSpec {
    name: "emoji",
    kind: ParamType::String,
    required: false,
    file: false,
};
const PAYLOAD: ParamSpec = ParamSpec {
    name: "payload",
    kind: ParamType::String,
    required: false,
    file: false,
};
const CURRENCY: ParamSpec = ParamSpec {
    name: "currency",
    kind: ParamType::String,
    required: false,
    file: false,
};
const PRICES: ParamSpec = ParamSpec {
    name: "prices",
    kind: ParamType::Array,
    required: false,
    file: false,
};
const MAX_TIP_AMOUNT: ParamSpec = ParamSpec {
    name: "max_tip_amount",
    kind: ParamType::Integer,
    required: false,
    file: false,
};
const SUGGESTED_TIP_AMOUNTS: ParamSpec = ParamSpec {
    name: "suggested_tip_amounts",
    kind: ParamType::Array,
    required: false,
    file: false,
};
const START_PARAMETER: ParamSpec = ParamSpec {
    name: "start_parameter",
    kind: ParamType::String,
    required: false,
    file: false,
};
const PROVIDER_TOKEN: ParamSpec = ParamSpec {
    name: "provider_token",
    kind: ParamType::String,
    required: false,
    file: false,
};
const PROVIDER_DATA: ParamSpec = ParamSpec {
    name: "provider_data",
    kind: ParamType::String,
    required: false,
    file: false,
};
const NEED_NAME: ParamSpec = ParamSpec {
    name: "need_name",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const NEED_PHONE_NUMBER: ParamSpec = ParamSpec {
    name: "need_phone_number",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const NEED_EMAIL: ParamSpec = ParamSpec {
    name: "need_email",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const NEED_SHIPPING_ADDRESS: ParamSpec = ParamSpec {
    name: "need_shipping_address",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const SEND_PHONE_NUMBER_TO_PROVIDER: ParamSpec = ParamSpec {
    name: "send_phone_number_to_provider",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const SEND_EMAIL_TO_PROVIDER: ParamSpec = ParamSpec {
    name: "send_email_to_provider",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const IS_FLEXIBLE: ParamSpec = ParamSpec {
    name: "is_flexible",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const UNTIL_DATE: ParamSpec = ParamSpec {
    name: "until_date",
    kind: ParamType::Integer,
    required: false,
    file: false,
};
const PERMISSIONS: ParamSpec = ParamSpec {
    name: "permissions",
    kind: ParamType::Object,
    required: false,
    file: false,
};
const CAN_SEND_MEDIA: ParamSpec = ParamSpec {
    name: "can_send_media",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const CAN_SEND_OTHER_MESSAGES: ParamSpec = ParamSpec {
    name: "can_send_other_messages",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const CAN_ADD_WEB_PAGE_PREVIEWS: ParamSpec = ParamSpec {
    name: "can_add_web_page_previews",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const INVITE_LINK: ParamSpec = ParamSpec {
    name: "invite_link",
    kind: ParamType::String,
    required: false,
    file: false,
};
const IS_PRIMARY: ParamSpec = ParamSpec {
    name: "is_primary",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const IS_REVOKED: ParamSpec = ParamSpec {
    name: "is_revoked",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const USER_ADMIN_RIGHTS: ParamSpec = ParamSpec {
    name: "user_administrator_rights",
    kind: ParamType::Object,
    required: false,
    file: false,
};
const MY_ADMIN_RIGHTS: ParamSpec = ParamSpec {
    name: "my_administrator_rights",
    kind: ParamType::Object,
    required: false,
    file: false,
};
const VERTICAL: ParamSpec = ParamSpec {
    name: "vertical",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const SET_NAME: ParamSpec = ParamSpec {
    name: "set_name",
    kind: ParamType::String,
    required: false,
    file: false,
};
const STICKER_TYPE: ParamSpec = ParamSpec {
    name: "sticker_type",
    kind: ParamType::String,
    required: false,
    file: false,
};
const NEEDS_REPAINTING: ParamSpec = ParamSpec {
    name: "needs_repainting",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const FORMAT: ParamSpec = ParamSpec {
    name: "format",
    kind: ParamType::String,
    required: false,
    file: false,
};
const MASK_POSITION: ParamSpec = ParamSpec {
    name: "mask_position",
    kind: ParamType::Object,
    required: false,
    file: false,
};
const KEYWORDS: ParamSpec = ParamSpec {
    name: "keywords",
    kind: ParamType::Array,
    required: false,
    file: false,
};
const CUSTOM_EMOJI_ID: ParamSpec = ParamSpec {
    name: "custom_emoji_id",
    kind: ParamType::String,
    required: false,
    file: false,
};
const POSITION: ParamSpec = ParamSpec {
    name: "position",
    kind: ParamType::Integer,
    required: false,
    file: false,
};
const INLINE_MESSAGE_ID: ParamSpec = ParamSpec {
    name: "inline_message_id",
    kind: ParamType::String,
    required: false,
    file: false,
};
const URL: ParamSpec = ParamSpec {
    name: "url",
    kind: ParamType::String,
    required: false,
    file: false,
};
const SHIPPING_OPTION_ID: ParamSpec = ParamSpec {
    name: "shipping_option_id",
    kind: ParamType::String,
    required: false,
    file: false,
};
const OK: ParamSpec = ParamSpec {
    name: "ok",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const ERROR_MESSAGE: ParamSpec = ParamSpec {
    name: "error_message",
    kind: ParamType::String,
    required: false,
    file: false,
};
const CACHE_TIME: ParamSpec = ParamSpec {
    name: "cache_time",
    kind: ParamType::Integer,
    required: false,
    file: false,
};
const IS_PERSONAL: ParamSpec = ParamSpec {
    name: "is_personal",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const NEXT_OFFSET: ParamSpec = ParamSpec {
    name: "next_offset",
    kind: ParamType::String,
    required: false,
    file: false,
};
const SWITCH_PM_TEXT: ParamSpec = ParamSpec {
    name: "switch_pm_text",
    kind: ParamType::String,
    required: false,
    file: false,
};
const SWITCH_PM_PARAMETER: ParamSpec = ParamSpec {
    name: "switch_pm_parameter",
    kind: ParamType::String,
    required: false,
    file: false,
};
const RESULTS: ParamSpec = ParamSpec {
    name: "results",
    kind: ParamType::Array,
    required: false,
    file: false,
};
const PREPARED_INLINE_MESSAGE_ID: ParamSpec = ParamSpec {
    name: "prepared_inline_message_id",
    kind: ParamType::String,
    required: false,
    file: false,
};
const COMMANDS: ParamSpec = ParamSpec {
    name: "commands",
    kind: ParamType::Array,
    required: false,
    file: false,
};
const SCOPE: ParamSpec = ParamSpec {
    name: "scope",
    kind: ParamType::Object,
    required: false,
    file: false,
};
const LANGUAGE_CODE: ParamSpec = ParamSpec {
    name: "language_code",
    kind: ParamType::String,
    required: false,
    file: false,
};
const DESCRIPTION: ParamSpec = ParamSpec {
    name: "description",
    kind: ParamType::String,
    required: false,
    file: false,
};
const SHORT_DESCRIPTION: ParamSpec = ParamSpec {
    name: "short_description",
    kind: ParamType::String,
    required: false,
    file: false,
};
const MENU_BUTTON: ParamSpec = ParamSpec {
    name: "menu_button",
    kind: ParamType::Object,
    required: false,
    file: false,
};
const RIGHTS: ParamSpec = ParamSpec {
    name: "rights",
    kind: ParamType::Object,
    required: false,
    file: false,
};
const CUSTOM_TITLE: ParamSpec = ParamSpec {
    name: "custom_title",
    kind: ParamType::String,
    required: false,
    file: false,
};
const SENDER_CHAT_ID: ParamSpec = ParamSpec {
    name: "sender_chat_id",
    kind: ParamType::StringOrInteger,
    required: false,
    file: false,
};
const CHAT_ID_REQ: ParamSpec = ParamSpec {
    name: "chat_id",
    kind: ParamType::StringOrInteger,
    required: true,
    file: false,
};
const USER_ID: ParamSpec = ParamSpec {
    name: "user_id",
    kind: ParamType::Integer,
    required: true,
    file: false,
};
const STAR_AMOUNT: ParamSpec = ParamSpec {
    name: "star_amount",
    kind: ParamType::Integer,
    required: false,
    file: false,
};
const TRANSACTION_ID: ParamSpec = ParamSpec {
    name: "transaction_id",
    kind: ParamType::String,
    required: false,
    file: false,
};
const MESSAGE_ID: ParamSpec = ParamSpec {
    name: "message_id",
    kind: ParamType::Integer,
    required: true,
    file: false,
};
const FROM_CHAT_ID: ParamSpec = ParamSpec {
    name: "from_chat_id",
    kind: ParamType::StringOrInteger,
    required: true,
    file: false,
};
const MEDIA_FILE: ParamSpec = ParamSpec {
    name: "media",
    kind: ParamType::InputFile,
    required: true,
    file: true,
};
const MEDIA_ARRAY: ParamSpec = ParamSpec {
    name: "media",
    kind: ParamType::Array,
    required: true,
    file: true,
};
const QUESTION_TEXT: ParamSpec = ParamSpec {
    name: "question",
    kind: ParamType::String,
    required: true,
    file: false,
};
const OPTIONS_ARRAY: ParamSpec = ParamSpec {
    name: "options",
    kind: ParamType::Array,
    required: true,
    file: false,
};
const PHONE_NUM: ParamSpec = ParamSpec {
    name: "phone_number",
    kind: ParamType::String,
    required: true,
    file: false,
};
const FIRST_NAME: ParamSpec = ParamSpec {
    name: "first_name",
    kind: ParamType::String,
    required: true,
    file: false,
};
const LATITUDE: ParamSpec = ParamSpec {
    name: "latitude",
    kind: ParamType::Float,
    required: true,
    file: false,
};
const LONGITUDE: ParamSpec = ParamSpec {
    name: "longitude",
    kind: ParamType::Float,
    required: true,
    file: false,
};
const ACTION_TYPE: ParamSpec = ParamSpec {
    name: "action",
    kind: ParamType::String,
    required: true,
    file: false,
};
const NAME_STR: ParamSpec = ParamSpec {
    name: "name",
    kind: ParamType::String,
    required: true,
    file: false,
};
const TITLE_STR: ParamSpec = ParamSpec {
    name: "title",
    kind: ParamType::String,
    required: true,
    file: false,
};
const STICKER_FILE: ParamSpec = ParamSpec {
    name: "sticker",
    kind: ParamType::InputFile,
    required: true,
    file: true,
};
// Sticker set-management methods take an existing sticker's file id (a
// String), not an upload; these must not flip the method into multipart.
const STICKER_ID: ParamSpec = ParamSpec {
    name: "sticker",
    kind: ParamType::String,
    required: true,
    file: false,
};
// addStickerToSet / replaceStickerInSet take an InputSticker object (which may
// embed a new file) under the official name `sticker`.
const INPUT_STICKER: ParamSpec = ParamSpec {
    name: "sticker",
    kind: ParamType::Object,
    required: true,
    file: false,
};
const MESSAGE_IDS: ParamSpec = ParamSpec {
    name: "message_ids",
    kind: ParamType::Array,
    required: true,
    file: false,
};
const PAYMENT_CHARGE_ID: ParamSpec = ParamSpec {
    name: "telegram_payment_charge_id",
    kind: ParamType::String,
    required: true,
    file: false,
};
const PAYLOAD_STR: ParamSpec = ParamSpec {
    name: "payload",
    kind: ParamType::String,
    required: false,
    file: false,
};
const GAME_SHORT_NAME: ParamSpec = ParamSpec {
    name: "game_short_name",
    kind: ParamType::String,
    required: false,
    file: false,
};
const SUPPORTS_STREAMING: ParamSpec = ParamSpec {
    name: "supports_streaming",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const LAST_NAME: ParamSpec = ParamSpec {
    name: "last_name",
    kind: ParamType::String,
    required: false,
    file: false,
};
const VCARD: ParamSpec = ParamSpec {
    name: "vcard",
    kind: ParamType::String,
    required: false,
    file: false,
};
const ADDRESS: ParamSpec = ParamSpec {
    name: "address",
    kind: ParamType::String,
    required: false,
    file: false,
};
const FOURSQUARE_ID: ParamSpec = ParamSpec {
    name: "foursquare_id",
    kind: ParamType::String,
    required: false,
    file: false,
};
const FOURSQUARE_TYPE: ParamSpec = ParamSpec {
    name: "foursquare_type",
    kind: ParamType::String,
    required: false,
    file: false,
};
const GOOGLE_PLACE_ID: ParamSpec = ParamSpec {
    name: "google_place_id",
    kind: ParamType::String,
    required: false,
    file: false,
};
const GOOGLE_PLACE_TYPE: ParamSpec = ParamSpec {
    name: "google_place_type",
    kind: ParamType::String,
    required: false,
    file: false,
};
const REACTION: ParamSpec = ParamSpec {
    name: "reaction",
    kind: ParamType::Array,
    required: false,
    file: false,
};
const IS_BIG: ParamSpec = ParamSpec {
    name: "is_big",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const CAPTION_ENTITIES: ParamSpec = ParamSpec {
    name: "caption_entities",
    kind: ParamType::Array,
    required: false,
    file: false,
};
const SHIPPING_QUERY_ID: ParamSpec = ParamSpec {
    name: "shipping_query_id",
    kind: ParamType::String,
    required: true,
    file: false,
};
const SHIPPING_OPTIONS: ParamSpec = ParamSpec {
    name: "shipping_options",
    kind: ParamType::Array,
    required: false,
    file: false,
};
const PRE_CHECKOUT_QUERY_ID: ParamSpec = ParamSpec {
    name: "pre_checkout_query_id",
    kind: ParamType::String,
    required: true,
    file: false,
};
const IS_CANCELED: ParamSpec = ParamSpec {
    name: "is_canceled",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const REVOKE_MESSAGES: ParamSpec = ParamSpec {
    name: "revoke_messages",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const ONLY_IF_BANNED: ParamSpec = ParamSpec {
    name: "only_if_banned",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const CAN_MANAGE_CHAT: ParamSpec = ParamSpec {
    name: "can_manage_chat",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const CAN_MANAGE_VIDEO_CHATS: ParamSpec = ParamSpec {
    name: "can_manage_video_chats",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const CAN_DELETE_MESSAGES: ParamSpec = ParamSpec {
    name: "can_delete_messages",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const CAN_RESTRICT_MEMBERS: ParamSpec = ParamSpec {
    name: "can_restrict_members",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const CAN_PROMOTE_MEMBERS: ParamSpec = ParamSpec {
    name: "can_promote_members",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const CAN_CHANGE_INFO: ParamSpec = ParamSpec {
    name: "can_change_info",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const CAN_INVITE_USERS: ParamSpec = ParamSpec {
    name: "can_invite_users",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const CAN_POST_MESSAGES: ParamSpec = ParamSpec {
    name: "can_post_messages",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const CAN_EDIT_MESSAGES: ParamSpec = ParamSpec {
    name: "can_edit_messages",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const CAN_PIN_MESSAGES: ParamSpec = ParamSpec {
    name: "can_pin_messages",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const CAN_MANAGE_TOPICS: ParamSpec = ParamSpec {
    name: "can_manage_topics",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const EXPIRE_DATE: ParamSpec = ParamSpec {
    name: "expire_date",
    kind: ParamType::Integer,
    required: false,
    file: false,
};
const MEMBER_LIMIT: ParamSpec = ParamSpec {
    name: "member_limit",
    kind: ParamType::Integer,
    required: false,
    file: false,
};
const CREATES_JOIN_REQUEST: ParamSpec = ParamSpec {
    name: "creates_join_request",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const STICKER_FORMAT: ParamSpec = ParamSpec {
    name: "sticker_format",
    kind: ParamType::String,
    required: false,
    file: false,
};
const STICKERS: ParamSpec = ParamSpec {
    name: "stickers",
    kind: ParamType::Array,
    required: true,
    file: false,
};
const OLD_STICKER: ParamSpec = ParamSpec {
    name: "old_sticker",
    kind: ParamType::String,
    required: true,
    file: false,
};
const NEW_STICKER: ParamSpec = ParamSpec {
    name: "new_sticker",
    kind: ParamType::InputFile,
    required: true,
    file: true,
};
const EMOJI_LIST: ParamSpec = ParamSpec {
    name: "emoji_list",
    kind: ParamType::Array,
    required: true,
    file: false,
};
const INLINE_QUERY_ID: ParamSpec = ParamSpec {
    name: "inline_query_id",
    kind: ParamType::String,
    required: true,
    file: false,
};
const BUTTON: ParamSpec = ParamSpec {
    name: "button",
    kind: ParamType::Object,
    required: false,
    file: false,
};
const WEB_APP_QUERY_ID_CONST: ParamSpec = ParamSpec {
    name: "web_app_query_id",
    kind: ParamType::String,
    required: true,
    file: false,
};
const RESULT_OBJ: ParamSpec = ParamSpec {
    name: "result",
    kind: ParamType::Object,
    required: true,
    file: false,
};
const ALLOW_USER_CHATS: ParamSpec = ParamSpec {
    name: "allow_user_chats",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const CALLBACK_QUERY_ID: ParamSpec = ParamSpec {
    name: "callback_query_id",
    kind: ParamType::String,
    required: true,
    file: false,
};
const SHOW_ALERT: ParamSpec = ParamSpec {
    name: "show_alert",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
const FOR_CHANNELS: ParamSpec = ParamSpec {
    name: "for_channels",
    kind: ParamType::Boolean,
    required: false,
    file: false,
};
#[allow(dead_code, clippy::allow_attributes)]
macro_rules! m { ($n:literal,$f:ident,$p:ident,[$($x:expr),*]) => {MethodSpec{name:$n,family:MethodFamily::$f,projection:SuccessProjection::$p,params:&[$($x),*]}} }
/// Bot API 10.3 outbound/control methods accepted by bulk dispatch. Inbound polling,
/// webhook management, introspection and bulk-service methods are intentionally absent.
pub static METHODS: &[MethodSpec] = &[
    m!(
        "sendMessage",
        Send,
        Message,
        [
            CHAT_ID_REQ,
            TEXT,
            PARSE_MODE,
            ENTITIES,
            LINK_PREVIEW_OPTS,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            REPLY_MARKUP,
            ALLOW_PAID,
            MESSAGE_THREAD_ID,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "forwardMessage",
        Forward,
        Message,
        [
            CHAT_ID_REQ,
            FROM_CHAT_ID,
            MESSAGE_ID,
            MESSAGE_THREAD_ID,
            DISABLE_NOTIF,
            PROTECT_CONTENT
        ]
    ),
    m!(
        "forwardMessages",
        Forward,
        FirstMessage,
        [
            CHAT_ID_REQ,
            FROM_CHAT_ID,
            MESSAGE_IDS,
            MESSAGE_THREAD_ID,
            DISABLE_NOTIF,
            PROTECT_CONTENT
        ]
    ),
    m!(
        "copyMessage",
        Copy,
        Message,
        [
            CHAT_ID_REQ,
            FROM_CHAT_ID,
            MESSAGE_ID,
            MESSAGE_THREAD_ID,
            CAPTION,
            PARSE_MODE,
            CAPTION_ENTITIES,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            REPLY_MARKUP
        ]
    ),
    m!(
        "copyMessages",
        Copy,
        FirstMessage,
        [
            CHAT_ID_REQ,
            FROM_CHAT_ID,
            MESSAGE_IDS,
            MESSAGE_THREAD_ID,
            DISABLE_NOTIF,
            PROTECT_CONTENT
        ]
    ),
    m!(
        "sendPhoto",
        Send,
        Message,
        [
            CHAT_ID_REQ,
            PHOTO,
            CAPTION,
            PARSE_MODE,
            CAPTION_ENTITIES,
            HAS_SPOILER,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            REPLY_MARKUP,
            ALLOW_PAID,
            MESSAGE_THREAD_ID,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "sendAudio",
        Send,
        Message,
        [
            CHAT_ID_REQ,
            AUDIO,
            CAPTION,
            PARSE_MODE,
            CAPTION_ENTITIES,
            DURATION,
            PERFORMER,
            TITLE,
            THUMBNAIL,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            REPLY_MARKUP,
            ALLOW_PAID,
            MESSAGE_THREAD_ID,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "sendDocument",
        Send,
        Message,
        [
            CHAT_ID_REQ,
            DOC,
            THUMBNAIL,
            CAPTION,
            PARSE_MODE,
            CAPTION_ENTITIES,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            REPLY_MARKUP,
            ALLOW_PAID,
            MESSAGE_THREAD_ID,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "sendVideo",
        Send,
        Message,
        [
            CHAT_ID_REQ,
            VIDEO,
            DURATION,
            WIDTH,
            HEIGHT,
            THUMBNAIL,
            CAPTION,
            PARSE_MODE,
            CAPTION_ENTITIES,
            HAS_SPOILER,
            SUPPORTS_STREAMING,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            REPLY_MARKUP,
            ALLOW_PAID,
            MESSAGE_THREAD_ID,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "sendAnimation",
        Send,
        Message,
        [
            CHAT_ID_REQ,
            ANIMATION,
            DURATION,
            WIDTH,
            HEIGHT,
            THUMBNAIL,
            CAPTION,
            PARSE_MODE,
            CAPTION_ENTITIES,
            HAS_SPOILER,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            REPLY_MARKUP,
            ALLOW_PAID,
            MESSAGE_THREAD_ID,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "sendVoice",
        Send,
        Message,
        [
            CHAT_ID_REQ,
            VOICE,
            CAPTION,
            PARSE_MODE,
            CAPTION_ENTITIES,
            DURATION,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            REPLY_MARKUP,
            ALLOW_PAID,
            MESSAGE_THREAD_ID,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "sendVideoNote",
        Send,
        Message,
        [
            CHAT_ID_REQ,
            NOTE,
            DURATION,
            WIDTH,
            HEIGHT,
            THUMBNAIL,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            REPLY_MARKUP,
            ALLOW_PAID,
            MESSAGE_THREAD_ID,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "sendMediaGroup",
        Send,
        FirstMessage,
        [
            CHAT_ID_REQ,
            MEDIA_ARRAY,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            MESSAGE_THREAD_ID,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "sendLocation",
        Send,
        Message,
        [
            CHAT_ID_REQ,
            LATITUDE,
            LONGITUDE,
            HORIZONTAL_ACCURACY,
            LIVE_PERIOD,
            HEADING,
            PROXIMITY_ALERT_RADIUS,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            REPLY_MARKUP,
            ALLOW_PAID,
            MESSAGE_THREAD_ID,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "sendVenue",
        Send,
        Message,
        [
            CHAT_ID_REQ,
            LATITUDE,
            LONGITUDE,
            TITLE_STR,
            ADDRESS,
            FOURSQUARE_ID,
            FOURSQUARE_TYPE,
            GOOGLE_PLACE_ID,
            GOOGLE_PLACE_TYPE,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            REPLY_MARKUP,
            ALLOW_PAID,
            MESSAGE_THREAD_ID,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "sendContact",
        Send,
        Message,
        [
            CHAT_ID_REQ,
            PHONE_NUM,
            FIRST_NAME,
            LAST_NAME,
            VCARD,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            REPLY_MARKUP,
            ALLOW_PAID,
            MESSAGE_THREAD_ID,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "sendPoll",
        Send,
        Message,
        [
            CHAT_ID_REQ,
            QUESTION_TEXT,
            OPTIONS_ARRAY,
            IS_ANONYMOUS,
            TYPE,
            ALLOWS_MULTIPLE_ANSWERS,
            CORRECT_OPTION_ID,
            EXPLANATION,
            EXPLANATION_PARSE_MODE,
            EXPLANATION_ENTITIES,
            OPEN_PERIOD,
            CLOSE_DATE,
            IS_CLOSED,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            REPLY_MARKUP,
            ALLOW_PAID,
            MESSAGE_THREAD_ID,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "sendDice",
        Send,
        Message,
        [
            CHAT_ID_REQ,
            EMOJI,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            REPLY_MARKUP,
            ALLOW_PAID,
            MESSAGE_THREAD_ID,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "sendChatAction",
        Send,
        None,
        [
            CHAT_ID_REQ,
            ACTION_TYPE,
            MESSAGE_THREAD_ID,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "setMessageReaction",
        Edit,
        None,
        [CHAT_ID_REQ, MESSAGE_ID, REACTION, IS_BIG, BUSINESS_CONN_ID]
    ),
    m!(
        "editMessageText",
        Edit,
        Message,
        [
            CHAT_ID_REQ,
            MESSAGE_ID,
            INLINE_MESSAGE_ID,
            TEXT,
            PARSE_MODE,
            ENTITIES,
            LINK_PREVIEW_OPTS,
            REPLY_MARKUP,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "editMessageCaption",
        Edit,
        Message,
        [
            CHAT_ID_REQ,
            MESSAGE_ID,
            INLINE_MESSAGE_ID,
            CAPTION,
            PARSE_MODE,
            CAPTION_ENTITIES,
            REPLY_MARKUP,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "editMessageMedia",
        Edit,
        Message,
        [
            CHAT_ID_REQ,
            MESSAGE_ID,
            INLINE_MESSAGE_ID,
            MEDIA_FILE,
            REPLY_MARKUP,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "editMessageLiveLocation",
        Edit,
        Message,
        [
            CHAT_ID_REQ,
            MESSAGE_ID,
            INLINE_MESSAGE_ID,
            LATITUDE,
            LONGITUDE,
            HORIZONTAL_ACCURACY,
            HEADING,
            PROXIMITY_ALERT_RADIUS,
            REPLY_MARKUP,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "stopMessageLiveLocation",
        Edit,
        Message,
        [
            CHAT_ID_REQ,
            MESSAGE_ID,
            INLINE_MESSAGE_ID,
            REPLY_MARKUP,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "editMessageReplyMarkup",
        Edit,
        Message,
        [
            CHAT_ID_REQ,
            MESSAGE_ID,
            INLINE_MESSAGE_ID,
            REPLY_MARKUP,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "stopPoll",
        Edit,
        None,
        [CHAT_ID_REQ, MESSAGE_ID, REPLY_MARKUP]
    ),
    m!("deleteMessage", Delete, None, [CHAT_ID_REQ, MESSAGE_ID]),
    m!("deleteMessages", Delete, None, [CHAT_ID_REQ, MESSAGE_IDS]),
    m!(
        "sendPaidMedia",
        Send,
        Message,
        [
            CHAT_ID_REQ,
            MEDIA_ARRAY,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            REPLY_MARKUP,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "sendChecklist",
        Send,
        Message,
        [
            CHAT_ID_REQ,
            TEXT,
            PARSE_MODE,
            ENTITIES,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            REPLY_MARKUP,
            MESSAGE_THREAD_ID,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "editMessageChecklist",
        Edit,
        Message,
        [
            CHAT_ID_REQ,
            MESSAGE_ID,
            TEXT,
            PARSE_MODE,
            ENTITIES,
            REPLY_MARKUP,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "sendInvoice",
        Payment,
        Message,
        [
            CHAT_ID_REQ,
            TITLE_STR,
            DESCRIPTION,
            PAYLOAD_STR,
            PROVIDER_TOKEN,
            CURRENCY,
            PRICES,
            MAX_TIP_AMOUNT,
            SUGGESTED_TIP_AMOUNTS,
            START_PARAMETER,
            PROVIDER_DATA,
            NEED_NAME,
            NEED_PHONE_NUMBER,
            NEED_EMAIL,
            NEED_SHIPPING_ADDRESS,
            SEND_PHONE_NUMBER_TO_PROVIDER,
            SEND_EMAIL_TO_PROVIDER,
            IS_FLEXIBLE,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            REPLY_MARKUP,
            MESSAGE_THREAD_ID,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "createInvoiceLink",
        Payment,
        None,
        [
            TITLE_STR,
            DESCRIPTION,
            PAYLOAD_STR,
            PROVIDER_TOKEN,
            CURRENCY,
            PRICES,
            MAX_TIP_AMOUNT,
            SUGGESTED_TIP_AMOUNTS,
            START_PARAMETER,
            PROVIDER_DATA,
            NEED_NAME,
            NEED_PHONE_NUMBER,
            NEED_EMAIL,
            NEED_SHIPPING_ADDRESS,
            SEND_PHONE_NUMBER_TO_PROVIDER,
            SEND_EMAIL_TO_PROVIDER,
            IS_FLEXIBLE
        ]
    ),
    m!(
        "answerShippingQuery",
        Payment,
        None,
        [SHIPPING_QUERY_ID, OK, SHIPPING_OPTIONS, ERROR_MESSAGE]
    ),
    m!(
        "answerPreCheckoutQuery",
        Payment,
        None,
        [PRE_CHECKOUT_QUERY_ID, OK, ERROR_MESSAGE]
    ),
    m!(
        "refundStarPayment",
        Payment,
        None,
        [USER_ID, PAYMENT_CHARGE_ID]
    ),
    m!(
        "editUserStarSubscription",
        Payment,
        None,
        [USER_ID, PAYMENT_CHARGE_ID, IS_CANCELED]
    ),
    m!(
        "banChatMember",
        Admin,
        None,
        [CHAT_ID_REQ, USER_ID, UNTIL_DATE, REVOKE_MESSAGES]
    ),
    m!(
        "unbanChatMember",
        Admin,
        None,
        [CHAT_ID_REQ, USER_ID, ONLY_IF_BANNED]
    ),
    m!(
        "restrictChatMember",
        Admin,
        None,
        [CHAT_ID_REQ, USER_ID, PERMISSIONS, UNTIL_DATE]
    ),
    m!(
        "promoteChatMember",
        Admin,
        None,
        [
            CHAT_ID_REQ,
            USER_ID,
            IS_ANONYMOUS,
            CAN_MANAGE_CHAT,
            CAN_MANAGE_VIDEO_CHATS,
            CAN_DELETE_MESSAGES,
            CAN_RESTRICT_MEMBERS,
            CAN_PROMOTE_MEMBERS,
            CAN_CHANGE_INFO,
            CAN_INVITE_USERS,
            CAN_POST_MESSAGES,
            CAN_EDIT_MESSAGES,
            CAN_PIN_MESSAGES,
            CAN_MANAGE_TOPICS
        ]
    ),
    m!(
        "setChatAdministratorCustomTitle",
        Admin,
        None,
        [CHAT_ID_REQ, USER_ID, CUSTOM_TITLE]
    ),
    m!(
        "banChatSenderChat",
        Admin,
        None,
        [CHAT_ID_REQ, SENDER_CHAT_ID]
    ),
    m!(
        "unbanChatSenderChat",
        Admin,
        None,
        [CHAT_ID_REQ, SENDER_CHAT_ID]
    ),
    m!(
        "setChatPermissions",
        Admin,
        None,
        [CHAT_ID_REQ, PERMISSIONS]
    ),
    m!("exportChatInviteLink", Admin, None, [CHAT_ID_REQ]),
    m!(
        "createChatInviteLink",
        Admin,
        None,
        [
            CHAT_ID_REQ,
            NAME_STR,
            EXPIRE_DATE,
            MEMBER_LIMIT,
            CREATES_JOIN_REQUEST
        ]
    ),
    m!(
        "editChatInviteLink",
        Admin,
        None,
        [
            CHAT_ID_REQ,
            INVITE_LINK,
            NAME_STR,
            EXPIRE_DATE,
            MEMBER_LIMIT,
            CREATES_JOIN_REQUEST
        ]
    ),
    m!(
        "revokeChatInviteLink",
        Admin,
        None,
        [CHAT_ID_REQ, INVITE_LINK]
    ),
    m!(
        "approveChatJoinRequest",
        Admin,
        None,
        [CHAT_ID_REQ, USER_ID]
    ),
    m!(
        "declineChatJoinRequest",
        Admin,
        None,
        [CHAT_ID_REQ, USER_ID]
    ),
    m!("setChatPhoto", Admin, None, [CHAT_ID_REQ, PHOTO]),
    m!("deleteChatPhoto", Admin, None, [CHAT_ID_REQ]),
    m!("setChatTitle", Admin, None, [CHAT_ID_REQ, TITLE_STR]),
    m!(
        "setChatDescription",
        Admin,
        None,
        [CHAT_ID_REQ, DESCRIPTION]
    ),
    m!(
        "pinChatMessage",
        Admin,
        None,
        [CHAT_ID_REQ, MESSAGE_ID, DISABLE_NOTIF]
    ),
    m!("unpinChatMessage", Admin, None, [CHAT_ID_REQ, MESSAGE_ID]),
    m!("unpinAllChatMessages", Admin, None, [CHAT_ID_REQ]),
    m!("leaveChat", Admin, None, [CHAT_ID_REQ]),
    m!(
        "sendSticker",
        Sticker,
        Message,
        [
            CHAT_ID_REQ,
            STICKER_FILE,
            EMOJI,
            DISABLE_NOTIF,
            PROTECT_CONTENT,
            REPLY_PARAMS,
            REPLY_MARKUP,
            ALLOW_PAID,
            MESSAGE_THREAD_ID,
            BUSINESS_CONN_ID
        ]
    ),
    m!(
        "uploadStickerFile",
        Sticker,
        None,
        [USER_ID, STICKER_FILE, STICKER_FORMAT]
    ),
    m!(
        "createNewStickerSet",
        Sticker,
        None,
        [
            USER_ID,
            NAME_STR,
            TITLE_STR,
            STICKER_TYPE,
            STICKERS,
            NEEDS_REPAINTING
        ]
    ),
    m!(
        "addStickerToSet",
        Sticker,
        None,
        [USER_ID, NAME_STR, INPUT_STICKER]
    ),
    m!(
        "setStickerPositionInSet",
        Sticker,
        None,
        [STICKER_ID, POSITION]
    ),
    m!("deleteStickerFromSet", Sticker, None, [STICKER_ID]),
    m!(
        "replaceStickerInSet",
        Sticker,
        None,
        [USER_ID, NAME_STR, OLD_STICKER, INPUT_STICKER]
    ),
    m!(
        "setStickerEmojiList",
        Sticker,
        None,
        [STICKER_ID, EMOJI_LIST]
    ),
    m!("setStickerKeywords", Sticker, None, [STICKER_ID, KEYWORDS]),
    m!(
        "setStickerMaskPosition",
        Sticker,
        None,
        [STICKER_ID, MASK_POSITION]
    ),
    m!("setStickerSetTitle", Sticker, None, [NAME_STR, TITLE_STR]),
    m!("deleteStickerSet", Sticker, None, [NAME_STR]),
    m!(
        "setStickerSetThumbnail",
        Sticker,
        None,
        [NAME_STR, USER_ID, THUMBNAIL, FORMAT]
    ),
    m!(
        "setCustomEmojiStickerSetThumbnail",
        Sticker,
        None,
        [NAME_STR, CUSTOM_EMOJI_ID]
    ),
    m!(
        "answerInlineQuery",
        Other,
        None,
        [
            INLINE_QUERY_ID,
            RESULTS,
            CACHE_TIME,
            IS_PERSONAL,
            NEXT_OFFSET,
            BUTTON
        ]
    ),
    m!(
        "answerWebAppQuery",
        Other,
        Message,
        [WEB_APP_QUERY_ID_CONST, RESULT_OBJ]
    ),
    m!(
        "savePreparedInlineMessage",
        Other,
        None,
        [USER_ID, RESULT_OBJ, ALLOW_USER_CHATS]
    ),
    m!(
        "answerCallbackQuery",
        Other,
        None,
        [CALLBACK_QUERY_ID, TEXT, SHOW_ALERT, URL, CACHE_TIME]
    ),
    m!(
        "setMyCommands",
        Other,
        None,
        [COMMANDS, SCOPE, LANGUAGE_CODE]
    ),
    m!("deleteMyCommands", Other, None, [SCOPE, LANGUAGE_CODE]),
    m!("setMyName", Other, None, [NAME_STR, LANGUAGE_CODE]),
    m!(
        "setMyDescription",
        Other,
        None,
        [DESCRIPTION, LANGUAGE_CODE]
    ),
    m!(
        "setMyShortDescription",
        Other,
        None,
        [SHORT_DESCRIPTION, LANGUAGE_CODE]
    ),
    m!("setChatMenuButton", Other, None, [CHAT_ID_REQ, MENU_BUTTON]),
    m!(
        "setMyDefaultAdministratorRights",
        Other,
        None,
        [RIGHTS, FOR_CHANNELS]
    ),
];

pub static NON_BULK_METHODS: &[&str] = &[
    "close",
    "logout",
    "getupdates",
    "setwebhook",
    "deletewebhook",
    "getwebhookinfo",
    "getme",
    "getfile",
    "getchat",
    "getchatadministrators",
    "getchatmembercount",
    "getchatmember",
    "getuserprofilephotos",
    "getstickerset",
    "getcustomemojistickers",
    "getforumtopiciconstickers",
    "getmycommands",
    "getmyname",
    "getmydescription",
    "getmyshortdescription",
    "getchatmenubutton",
    "getmydefaultadministratorrights",
    "getstartransactions",
    "getavailablegifts",
    "getbusinessconnection",
    "getgamehighscores",
    "getuserchatboosts",
];

pub fn method(name: &str) -> Option<&'static MethodSpec> {
    METHODS.iter().find(|m| m.name.eq_ignore_ascii_case(name))
}
pub fn is_non_bulk(name: &str) -> bool {
    NON_BULK_METHODS
        .iter()
        .any(|m| m.eq_ignore_ascii_case(name))
}
pub fn is_multipart(spec: &MethodSpec) -> bool {
    spec.params.iter().any(|p| p.file)
}
pub fn lease_ttl_secs(spec: &MethodSpec) -> u16 {
    if is_multipart(spec) {
        70
    } else {
        30
    }
}
pub fn method_timeout_secs(spec: &MethodSpec) -> u16 {
    if is_multipart(spec) {
        60
    } else {
        20
    }
}
pub fn can_auto_migrate(spec: &MethodSpec) -> bool {
    spec.params.iter().any(|p| p.name == "chat_id")
        && matches!(
            spec.family,
            MethodFamily::Send
                | MethodFamily::Copy
                | MethodFamily::Forward
                | MethodFamily::Edit
                | MethodFamily::Delete
        )
}
pub fn method_aware_retries(spec: &MethodSpec) -> bool {
    matches!(
        spec.family,
        MethodFamily::Send | MethodFamily::Copy | MethodFamily::Forward
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn representative_catalog() {
        for n in [
            "sendMessage",
            "sendPhoto",
            "sendMediaGroup",
            "copyMessage",
            "forwardMessage",
            "sendLocation",
            "sendPoll",
            "sendContact",
            "editMessageText",
            "deleteMessage",
            "banChatMember",
            "answerInlineQuery",
        ] {
            assert!(method(n).is_some(), "{n}");
        }
        assert!(is_non_bulk("getUpdates"));
        assert_eq!(lease_ttl_secs(method("sendPhoto").unwrap()), 70);
        assert_eq!(lease_ttl_secs(method("sendMessage").unwrap()), 30);
    }
    #[test]
    fn control_methods_are_not_bulk_dispatchable() {
        // close/logOut are bot session-control methods with no recipient
        // fan-out axis: they must be rejected (as non-bulk), never dispatched.
        assert!(method("close").is_none());
        assert!(method("logOut").is_none());
        assert!(is_non_bulk("close"));
        assert!(is_non_bulk("logOut"));
    }
    #[test]
    fn plural_methods_use_message_ids_array() {
        for name in ["forwardMessages", "copyMessages", "deleteMessages"] {
            let spec = method(name).expect(name);
            assert!(
                spec.params.iter().any(|p| p.name == "message_ids"),
                "{name} must declare message_ids"
            );
            assert!(
                !spec.params.iter().any(|p| p.name == "message_id"),
                "{name} must not declare singular message_id"
            );
            assert_eq!(lease_ttl_secs(spec), 30, "{name} is not a file upload");
        }
    }
    #[test]
    fn sticker_set_management_is_not_an_upload() {
        for name in [
            "setStickerPositionInSet",
            "deleteStickerFromSet",
            "setStickerEmojiList",
            "setStickerKeywords",
            "setStickerMaskPosition",
        ] {
            let spec = method(name).expect(name);
            let sticker = spec
                .params
                .iter()
                .find(|p| p.name == "sticker")
                .unwrap_or_else(|| panic!("{name} declares sticker"));
            assert!(!sticker.file, "{name}: sticker is a file-id String");
            assert!(!is_multipart(spec), "{name} is a JSON call, not multipart");
        }
        // addStickerToSet / replaceStickerInSet carry the InputSticker under the
        // official name `sticker`, and old_sticker is a String identifier.
        let add = method("addStickerToSet").unwrap();
        assert!(add.params.iter().any(|p| p.name == "sticker"));
        assert!(!add.params.iter().any(|p| p.name == "stickers"));
        let rep = method("replaceStickerInSet").unwrap();
        assert!(rep.params.iter().any(|p| p.name == "old_sticker"));
        assert!(rep.params.iter().any(|p| p.name == "sticker"));
        assert!(!rep.params.iter().any(|p| p.name == "new_sticker"));
        let old = rep.params.iter().find(|p| p.name == "old_sticker").unwrap();
        assert!(!old.file, "old_sticker is a String identifier");
    }
    #[test]
    fn star_payments_use_telegram_payment_charge_id() {
        for name in ["refundStarPayment", "editUserStarSubscription"] {
            let spec = method(name).expect(name);
            assert!(
                spec.params
                    .iter()
                    .any(|p| p.name == "telegram_payment_charge_id"),
                "{name} must use telegram_payment_charge_id"
            );
            assert!(!spec.params.iter().any(|p| p.name == "star_amount"));
            assert!(!spec.params.iter().any(|p| p.name == "transaction_id"));
        }
    }
    #[test]
    fn committed_fixture_is_in_sync_with_catalog() {
        // The committed fixture must be a byte-for-byte serde projection of
        // METHODS and NON_BULK_METHODS (the generator writes exactly this).
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../fixtures/bot-api-10.3.json")).unwrap();
        assert_eq!(
            fixture["methods"],
            serde_json::to_value(METHODS).unwrap(),
            "fixture METHODS drifted from the catalog; run the gen crate"
        );
        assert_eq!(
            fixture["non_bulk_methods"],
            serde_json::to_value(NON_BULK_METHODS).unwrap(),
            "fixture NON_BULK_METHODS drifted from the catalog; run the gen crate"
        );
        assert_eq!(fixture["bot_api_version"], "10.3");
    }
}
