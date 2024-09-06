import json
import re

def to_string(obj: dict) -> str:
    return json.dumps(obj)

def to_dict(obj: str) -> dict:
    return json.loads(obj)

def contains_format_flags(message: str):
    pattern = re.compile(r"\{(\w+)\}")
    matches = pattern.findall(message)
    allowed_keywords = ("first", "last", "full", "id", "chatname", "username", "mention")

    for keyword in matches:
        if keyword not in allowed_keywords:
            return False
    return True

def message_formatter(message: str, update, new: tuple[bool, bool]) -> str:
    user = update.chat_member.old_chat_member.user if not new[0] else update.chat_member.new_chat_member.user

    substitutions = {
        "first":  user.first_name or "",
        "last":  user.last_name or "",
        "full":  user.full_name or "",
        "id":  user.id or "",
        "chatname": update.effective_chat.title or "",
        "username":  user.username or "",
        "mention":  user.mention_html() or ""
    }

    if contains_format_flags(message):
        return message.format(**substitutions)
    
