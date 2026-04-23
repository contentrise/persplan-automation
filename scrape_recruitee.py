#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import logging
import re
import subprocess
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urldefrag, urljoin, urlparse

from playwright.async_api import Browser, Error as PlaywrightError, Page, async_playwright


BASE_URL = "https://apidocs.recruitee.com/"
OUTPUT_ROOT = Path("recruitee-api")
CHUNKS_DIR = OUTPUT_ROOT / "chunks"
MAX_PAGES = 50
REQUEST_TIMEOUT_MS = 120_000
SKIP_CRAWL_SUFFIXES = {
    ".css",
    ".gif",
    ".ico",
    ".jpeg",
    ".jpg",
    ".js",
    ".json",
    ".map",
    ".pdf",
    ".png",
    ".svg",
    ".txt",
    ".woff",
    ".woff2",
    ".xml",
}
GENERIC_PATH_SEGMENTS = {
    "api",
    "apis",
    "bulk",
    "c",
    "new",
    "oauth",
    "old",
    "search",
    "v1",
    "v2",
    "v3",
    "web",
}
GENERIC_SECTION_TOKENS = {
    "api",
    "auth",
    "bulk",
    "mobile",
    "public",
    "web",
}
OPERATION_SEGMENTS = {
    "add",
    "accept",
    "activate",
    "approve",
    "archive",
    "assign",
    "authorize",
    "calculate",
    "cancel",
    "change",
    "change_stage",
    "close",
    "conceal",
    "copy",
    "create",
    "deactivate",
    "delete",
    "disable",
    "disconnect",
    "dismiss",
    "disqualify",
    "draft",
    "enable",
    "exchange",
    "export",
    "fetch",
    "fill",
    "follow",
    "hide",
    "import",
    "integrate",
    "leave",
    "lock",
    "mark",
    "merge",
    "move",
    "publish",
    "preview",
    "pin",
    "reject",
    "register",
    "remove",
    "restore",
    "resend",
    "resync",
    "requalify",
    "retrieve",
    "reveal",
    "revert",
    "revoke",
    "restore",
    "schedule",
    "search",
    "select",
    "send",
    "set",
    "share",
    "show",
    "sign",
    "start",
    "stop",
    "sync",
    "trade",
    "toggle",
    "unarchive",
    "unassign",
    "unfollow",
    "unpin",
    "unpublish",
    "unlock",
    "update",
    "verify",
}


@dataclass
class Parameter:
    name: str
    type: str | None
    required: bool
    description: str
    source: str


@dataclass
class EndpointChunk:
    id: str
    method: str
    path: str
    summary: str
    description: str
    params: list[dict[str, Any]]
    example_request: str
    example_response: Any
    tags: list[str]


def normalize_whitespace(value: str | None) -> str:
    if not value:
        return ""
    text = value.replace("\xa0", " ")
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def slugify(value: str) -> str:
    text = normalize_whitespace(value)
    text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", text)
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    text = text.lower()
    text = text.replace("/", " ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def singularize(word: str) -> str:
    if word.endswith("ies") and len(word) > 3:
        return f"{word[:-3]}y"
    if word.endswith("ses") and len(word) > 3:
        return word[:-2]
    if word.endswith("s") and not word.endswith("ss") and len(word) > 1:
        return word[:-1]
    return word


def pluralize(word: str) -> str:
    if word.endswith("ies"):
        return word
    if word.endswith("y") and len(word) > 1 and word[-2] not in "aeiou":
        return f"{word[:-1]}ies"
    if word.endswith("s"):
        return word
    return f"{word}s"


def safe_json_loads(value: str | None) -> Any | None:
    if not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def meaningful_path_segments(path: str) -> list[str]:
    raw_segments = [segment for segment in path.split("/") if segment]
    segments: list[str] = []
    for segment in raw_segments:
        if segment.startswith("{") and segment.endswith("}"):
            continue
        cleaned = slugify(segment)
        if not cleaned:
            continue
        segments.append(cleaned)
    return segments


def canonical_tag(tag: str) -> str:
    return slugify(tag)


def tag_equivalent(left: str, right: str) -> bool:
    left_normalized = canonical_tag(left).replace("_", "")
    right_normalized = canonical_tag(right).replace("_", "")
    return (
        left_normalized == right_normalized
        or singularize(left_normalized) == singularize(right_normalized)
    )


def is_operation_segment(segment: str) -> bool:
    normalized = slugify(segment)
    if not normalized:
        return False
    if normalized in OPERATION_SEGMENTS:
        return True
    first_token = normalized.split("_", 1)[0]
    return first_token in OPERATION_SEGMENTS


def detect_path_tags(path: str) -> list[str]:
    segments = meaningful_path_segments(path)
    filtered = [segment for segment in segments if segment not in GENERIC_PATH_SEGMENTS]
    if not filtered:
        filtered = segments
    if not filtered:
        return ["misc"]

    tags: list[str] = []
    for segment in filtered:
        if is_operation_segment(segment):
            continue
        if any(tag_equivalent(segment, existing) for existing in tags):
            continue
        tags.append(segment)
        if len(tags) >= 2:
            break

    if tags:
        return tags
    return [filtered[0]]


def detect_section_tag(section_heading: str) -> str | None:
    if not section_heading:
        return None
    tokens = [slugify(token) for token in re.split(r"[.\s]+", section_heading)]
    for token in tokens:
        if token and token not in GENERIC_SECTION_TOKENS:
            return pluralize(token) if token in {"candidate", "offer", "job"} else token
    return None


def choose_tags(section_heading: str, path: str) -> list[str]:
    tags = detect_path_tags(path)
    section_tag = canonical_tag(detect_section_tag(section_heading) or "")
    if section_tag and not any(tag_equivalent(section_tag, tag) for tag in tags):
        tags.append(section_tag)
    return tags or ["misc"]


def is_detail_endpoint(path: str) -> bool:
    segments = [segment for segment in path.split("/") if segment]
    return bool(segments and segments[-1].startswith("{") and segments[-1].endswith("}"))


def infer_resource_name(path: str, tags: list[str]) -> str:
    segments = meaningful_path_segments(path)
    filtered = [segment for segment in segments if segment not in GENERIC_PATH_SEGMENTS]
    if filtered:
        for segment in reversed(filtered):
            if not is_operation_segment(segment):
                return segment
    return tags[0] if tags else "endpoint"


def infer_operation_name(method: str, path: str, summary: str) -> str:
    normalized_summary = slugify(re.sub(r"^\[[^\]]+\]\s*", "", summary))
    summary_rules = [
        (("list", "lists"), "list"),
        (("get", "gets", "show", "shows", "return", "returns", "receive", "receives"), "detail"),
        (("create", "creates"), "create"),
        (("update", "updates"), "update"),
        (("delete", "deletes", "remove", "removes"), "delete"),
        (("assign", "assigns"), "assign"),
        (("move", "moves"), "move"),
        (("archive", "archives"), "archive"),
        (("add", "adds"), "add"),
        (("approve", "approves"), "approve"),
        (("reject", "rejects"), "reject"),
        (("publish", "publishes"), "publish"),
        (("preview", "previews"), "preview"),
        (("pin", "pins"), "pin"),
        (("unpublish", "unpublishes"), "unpublish"),
        (("restore", "restores"), "restore"),
        (("send", "sends"), "send"),
        (("enable", "enables"), "enable"),
        (("disable", "disables"), "disable"),
        (("copy", "copies"), "copy"),
        (("search", "searches"), "search"),
        (("calculate", "calculates"), "calculate"),
        (("change", "changes"), "change"),
        (("conceal", "conceals"), "conceal"),
        (("deactivate", "deactivates"), "deactivate"),
        (("disconnect", "disconnects"), "disconnect"),
        (("dismiss", "dismisses"), "dismiss"),
        (("disqualify", "disqualifies"), "disqualify"),
        (("draft", "drafts"), "draft"),
        (("exchange", "exchanges", "trade"), "exchange"),
        (("duplicate", "duplicates"), "duplicate"),
        (("fetch", "fetches"), "fetch"),
        (("fill", "fills"), "fill"),
        (("follow", "follows"), "follow"),
        (("hide", "hides"), "hide"),
        (("integrate", "integrates"), "integrate"),
        (("leave", "leaves"), "leave"),
        (("lock", "locks"), "lock"),
        (("mark", "marks", "marked"), "mark"),
        (("merge", "merges"), "merge"),
        (("open", "opens"), "open"),
        (("register", "registers"), "register"),
        (("reactivate", "reactivates"), "reactivate"),
        (("resend", "resends"), "resend"),
        (("resync", "resyncs"), "resync"),
        (("requalify", "requalifies"), "requalify"),
        (("revoke", "revokes"), "revoke"),
        (("retrieve", "retrieves"), "retrieve"),
        (("reveal", "reveals"), "reveal"),
        (("revert", "reverts"), "revert"),
        (("schedule", "schedules"), "schedule"),
        (("select", "selects"), "select"),
        (("set", "sets"), "set"),
        (("share", "shares"), "share"),
        (("sign", "signs"), "sign"),
        (("start", "starts"), "start"),
        (("stop", "stops"), "stop"),
        (("toggle", "toggles"), "toggle"),
        (("unarchive", "unarchives"), "unarchive"),
        (("unassign", "unassigns"), "unassign"),
        (("unfollow", "unfollows"), "unfollow"),
        (("unpin", "unpins"), "unpin"),
        (("close", "closes"), "close"),
    ]
    for prefixes, operation in summary_rules:
        if any(normalized_summary == prefix or normalized_summary.startswith(f"{prefix}_") for prefix in prefixes):
            return operation

    path_segments = [segment for segment in meaningful_path_segments(path) if segment not in GENERIC_PATH_SEGMENTS]
    tail = path_segments[-1] if path_segments else ""
    detail = is_detail_endpoint(path)
    if method == "GET":
        if detail or re.search(r"\b(current|single)\b", normalized_summary):
            return "detail"
        return "list"
    if method == "POST":
        if tail in {"authorize", "assign", "export", "import"}:
            return tail
        return "create"
    if method in {"PUT", "PATCH"}:
        if tail and tail not in {"update", "patch"} and not detail and tail not in {"candidates", "offers", "jobs"}:
            summary_slug = slugify(summary)
            if tail in summary_slug:
                return tail
        return "update"
    if method == "DELETE":
        return "delete"
    return slugify(summary) or method.lower()


def build_endpoint_id(method: str, path: str, summary: str, tags: list[str]) -> str:
    resource = infer_resource_name(path, tags)
    operation = infer_operation_name(method, path, summary)

    if operation == "list":
        return f"{pluralize(resource)}_list"
    if operation == "detail":
        return f"{singularize(resource)}_detail"
    if operation in {"create", "update", "delete"}:
        return f"{singularize(resource)}_{operation}"
    return f"{singularize(resource)}_{slugify(operation)}"


def extract_summary_object_phrase(summary: str, operation: str) -> str:
    summary_slug = slugify(re.sub(r"^\[[^\]]+\]\s*", "", summary))
    if not summary_slug:
        return ""

    operation_prefixes = {
        "list": ["list", "lists"],
        "detail": ["get", "gets", "show", "shows", "return", "returns", "receive", "receives"],
        "create": ["create", "creates"],
        "update": ["update", "updates"],
        "delete": ["delete", "deletes", "remove", "removes"],
        "assign": ["assign", "assigns"],
        "approve": ["approve", "approves"],
        "archive": ["archive", "archives"],
        "add": ["add", "adds"],
        "calculate": ["calculate", "calculates"],
        "cancel": ["cancel", "cancels"],
        "change": ["change", "changes"],
        "close": ["close", "closes"],
        "conceal": ["conceal", "conceals"],
        "copy": ["copy", "copies"],
        "deactivate": ["deactivate", "deactivates"],
        "disconnect": ["disconnect", "disconnects"],
        "dismiss": ["dismiss", "dismisses"],
        "disable": ["disable", "disables"],
        "disqualify": ["disqualify", "disqualifies"],
        "draft": ["draft", "drafts"],
        "duplicate": ["duplicate", "duplicates"],
        "enable": ["enable", "enables"],
        "exchange": ["exchange", "exchanges", "trade"],
        "fetch": ["fetch", "fetches"],
        "fill": ["fill", "fills"],
        "follow": ["follow", "follows"],
        "hide": ["hide", "hides"],
        "integrate": ["integrate", "integrates"],
        "leave": ["leave", "leaves"],
        "lock": ["lock", "locks"],
        "mark": ["mark", "marks", "marked"],
        "merge": ["merge", "merges"],
        "move": ["move", "moves"],
        "open": ["open", "opens"],
        "pin": ["pin", "pins"],
        "preview": ["preview", "previews"],
        "publish": ["publish", "publishes"],
        "reactivate": ["reactivate", "reactivates"],
        "register": ["register", "registers"],
        "reject": ["reject", "rejects"],
        "resend": ["resend", "resends"],
        "resync": ["resync", "resyncs"],
        "requalify": ["requalify", "requalifies"],
        "retrieve": ["retrieve", "retrieves"],
        "reveal": ["reveal", "reveals"],
        "revert": ["revert", "reverts"],
        "restore": ["restore", "restores"],
        "revoke": ["revoke", "revokes"],
        "schedule": ["schedule", "schedules"],
        "search": ["search", "searches"],
        "select": ["select", "selects"],
        "send": ["send", "sends"],
        "set": ["set", "sets"],
        "share": ["share", "shares"],
        "sign": ["sign", "signs"],
        "start": ["start", "starts"],
        "stop": ["stop", "stops"],
        "toggle": ["toggle", "toggles"],
        "unarchive": ["unarchive", "unarchives"],
        "unassign": ["unassign", "unassigns"],
        "unfollow": ["unfollow", "unfollows"],
        "unpin": ["unpin", "unpins"],
        "unpublish": ["unpublish", "unpublishes"],
        "verify": ["verify", "verifies"],
    }
    prefixes = operation_prefixes.get(operation, [operation])
    object_phrase = summary_slug
    for prefix in prefixes:
        if object_phrase == prefix:
            object_phrase = ""
            break
        if object_phrase.startswith(f"{prefix}_"):
            object_phrase = object_phrase[len(prefix) + 1 :]
            break

    cleanup_phrases = [
        "_for_current_admin",
        "_for_current_company",
        "_for_current_user",
        "_for_given_offer",
        "_for_given_offer_template",
        "_for_company",
        "_for_candidate",
        "_for_offer",
        "_for_requisition",
        "_to_company",
    ]
    for phrase in cleanup_phrases:
        object_phrase = object_phrase.replace(phrase, "")

    cleanup_words = {
        "a",
        "all",
        "an",
        "based",
        "current",
        "empty",
        "for",
        "given",
        "needed",
        "of",
        "single",
        "the",
        "this",
        "url",
        "used",
    }
    cleaned_tokens = [token for token in object_phrase.split("_") if token and token not in cleanup_words]
    return "_".join(cleaned_tokens)


def build_intent_name(chunk: EndpointChunk) -> str:
    resource = infer_resource_name(chunk.path, chunk.tags)
    operation = infer_operation_name(chunk.method, chunk.path, chunk.summary)
    object_phrase = extract_summary_object_phrase(chunk.summary, operation)

    if operation == "list":
        return f"list_{object_phrase or pluralize(resource)}"
    if operation == "detail":
        return f"get_{object_phrase or singularize(resource)}"
    if operation == "create":
        return f"create_{object_phrase or singularize(resource)}"
    if operation == "update":
        return f"update_{object_phrase or singularize(resource)}"
    if operation == "delete":
        return f"delete_{object_phrase or singularize(resource)}"
    return f"{slugify(operation)}_{object_phrase or singularize(resource)}"


def format_request_example(sections: list[dict[str, str]]) -> str:
    parts: list[str] = []
    for section in sections:
        label = normalize_whitespace(section.get("label"))
        text = normalize_whitespace(section.get("text"))
        if not text:
            continue
        if label:
            parts.append(f"{label}:\n{text}")
        else:
            parts.append(text)
    return "\n\n".join(parts).strip()


def normalize_param(raw_param: dict[str, Any]) -> Parameter | None:
    name = slugify(raw_param.get("name") or "")
    if not name:
        return None
    param_type = normalize_whitespace(raw_param.get("type")) or None
    return Parameter(
        name=name,
        type=param_type,
        required=bool(raw_param.get("required")),
        description=normalize_whitespace(raw_param.get("description")),
        source=normalize_whitespace(raw_param.get("source")),
    )


def normalize_response(raw_body: str) -> Any:
    body = normalize_whitespace(raw_body)
    if not body:
        return {}
    parsed = safe_json_loads(body)
    if parsed is not None:
        return parsed
    return body


def is_trivial_request_example(value: str) -> bool:
    normalized = normalize_whitespace(value)
    return normalized in {
        "",
        "Headers:\nContent-Type: application/json",
        "Headers:\nContent-Type: application/json\n\nBody:\n{}",
    }


def synthesize_example_request(method: str, path: str, params: list[dict[str, Any]], current: str) -> str:
    if current and not is_trivial_request_example(current):
        return current

    required = [param["name"] for param in params if param.get("required")]
    optional = [param["name"] for param in params if not param.get("required")]

    parts = [f"{method} {path}"]
    if required:
        parts.append(f"Required params: {', '.join(required)}")
    if optional:
        parts.append(f"Optional params: {', '.join(optional[:8])}")
    return "\n\n".join(part for part in parts if part).strip()


def synthesize_description(summary: str, method: str, path: str, params: list[dict[str, Any]], current: str) -> str:
    description = normalize_whitespace(current)
    if description:
        return description

    required = [param["name"] for param in params if param.get("required")]
    optional = [param["name"] for param in params if not param.get("required")]

    sentences = [f"{summary}.", f"Endpoint: {method} {path}."]
    if required:
        sentences.append(f"Required parameters: {', '.join(required)}.")
    if optional:
        preview = ", ".join(optional[:8])
        suffix = "." if len(optional) <= 8 else ", and more."
        sentences.append(f"Optional parameters include {preview}{suffix}")
    return " ".join(sentences)


class RecruiteeCrawler:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self.base_host = urlparse(base_url).netloc

    def normalize_url(self, candidate: str) -> str | None:
        if not candidate:
            return None
        absolute, _fragment = urldefrag(urljoin(self.base_url, candidate))
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            return None
        if parsed.netloc != self.base_host:
            return None
        if any(parsed.path.lower().endswith(suffix) for suffix in SKIP_CRAWL_SUFFIXES):
            return None
        path = parsed.path or "/"
        normalized = parsed._replace(path=path, params="", query="", fragment="")
        return normalized.geturl()

    async def _load_page(self, page: Page, url: str) -> None:
        logging.info("Crawling %s", url)
        await page.goto(url, wait_until="networkidle", timeout=REQUEST_TIMEOUT_MS)
        await page.wait_for_timeout(500)

    async def crawl(self, browser: Browser) -> list[str]:
        context = await browser.new_context(ignore_https_errors=True)
        try:
            queue = [self.base_url]
            visited: set[str] = set()

            while queue and len(visited) < MAX_PAGES:
                current = queue.pop(0)
                normalized = self.normalize_url(current)
                if not normalized or normalized in visited:
                    continue

                page = await context.new_page()
                try:
                    await self._load_page(page, normalized)
                    visited.add(normalized)
                    links = await page.eval_on_selector_all(
                        "a[href]",
                        """elements => elements.map(element => element.getAttribute('href') || '')""",
                    )
                    for href in links:
                        candidate = self.normalize_url(href)
                        if candidate and candidate not in visited and candidate not in queue:
                            queue.append(candidate)
                finally:
                    await page.close()

            urls = sorted(visited) or [self.base_url]
            logging.info("Discovered %s documentation page(s)", len(urls))
            return urls
        finally:
            await context.close()


class RecruiteeParser:
    EXTRACT_SCRIPT = """
    (sourceUrl) => {
      const clean = (value) => {
        if (!value) return '';
        return value
          .replace(/\\u00a0/g, ' ')
          .replace(/\\r\\n?/g, '\\n')
          .replace(/[ \\t]+/g, ' ')
          .replace(/ *\\n */g, '\\n')
          .replace(/\\n{3,}/g, '\\n\\n')
          .trim();
      };

      const extractSections = (tabElement) => {
        if (!tabElement) return [];
        const inner = tabElement.querySelector(':scope > div > .inner') || tabElement.querySelector('.inner');
        if (!inner) return [];

        const sections = [];
        let currentLabel = '';

        for (const child of Array.from(inner.children)) {
          if (child.tagName === 'H5') {
            currentLabel = clean(child.innerText);
          } else if (child.tagName === 'PRE') {
            sections.push({
              label: currentLabel,
              text: clean(child.innerText),
            });
          }
        }

        return sections;
      };

      const pickTab = (tabsContainer, preferredMatcher) => {
        if (!tabsContainer) return { label: '', element: null };

        const buttons = Array.from(tabsContainer.querySelectorAll(':scope > .example-names .tab-button'));
        const tabs = Array.from(tabsContainer.children).filter((child) => child.classList && child.classList.contains('tab'));
        if (!tabs.length) return { label: '', element: null };

        let index = buttons.findIndex((button) => preferredMatcher && preferredMatcher(clean(button.innerText)));
        if (index < 0) {
          index = buttons.findIndex((button) => button.classList.contains('active'));
        }
        if (index < 0) {
          index = 0;
        }

        return {
          label: clean(buttons[index] ? buttons[index].innerText : ''),
          element: tabs[index] || tabs[0] || null,
        };
      };

      const extractExamples = (rightPanel) => {
        if (!rightPanel) {
          return {
            request_sections: [],
            request_label: '',
            response_sections: [],
            response_label: '',
          };
        }

        const requestContainer = rightPanel.querySelector(':scope > .tabs');
        const requestChoice = pickTab(requestContainer, (label) => /json/i.test(label));
        const requestSections = extractSections(requestChoice.element);

        let responseChoice = { label: '', element: null };
        if (requestChoice.element) {
          const nestedTabs = Array.from(requestChoice.element.children).find(
            (child) => child.classList && child.classList.contains('tabs')
          );
          responseChoice = pickTab(nestedTabs, (label) => /^2\\d\\d$/.test(label) || /json/i.test(label));
        }

        return {
          request_sections: requestSections,
          request_label: requestChoice.label,
          response_sections: extractSections(responseChoice.element),
          response_label: responseChoice.label,
        };
      };

      const extractParams = (action) => {
        const params = [];
        for (const title of Array.from(action.querySelectorAll(':scope > .title'))) {
          const source = clean((title.querySelector('strong') || title).innerText.replace(/\\b(Hide|Show)\\b/g, ''));
          const content = title.nextElementSibling;
          if (!content || !content.classList.contains('collapse-content')) continue;

          for (const dt of Array.from(content.querySelectorAll(':scope > dl > dt'))) {
            const dd = dt.nextElementSibling;
            if (!dd) continue;

            const descriptionNodes = Array.from(dd.querySelectorAll('p'));
            params.push({
              name: clean(dt.innerText),
              type: clean(dd.querySelector('code') ? dd.querySelector('code').innerText : ''),
              required: /\\(required\\)/i.test(dd.innerText),
              description: clean(descriptionNodes.map((node) => node.innerText).join('\\n')),
              source,
            });
          }
        }
        return params;
      };

      const actions = Array.from(document.querySelectorAll('div.action'));
      return actions.map((action, index) => {
        const heading = action.querySelector('h4.action-heading');
        const middle = action.parentElement;
        const rightPanel = middle && middle.previousElementSibling && middle.previousElementSibling.classList.contains('right')
          ? middle.previousElementSibling
          : null;

        let sectionHeading = '';
        let cursor = rightPanel ? rightPanel.previousElementSibling : middle ? middle.previousElementSibling : null;
        while (cursor) {
          const resourceHeading = cursor.querySelector('h3.resource-heading, h2.group-heading');
          if (resourceHeading) {
            sectionHeading = clean(resourceHeading.innerText.replace('¶', ''));
            break;
          }
          cursor = cursor.previousElementSibling;
        }

        const directParagraphs = Array.from(action.children)
          .filter((child) => child.tagName === 'P')
          .map((child) => clean(child.innerText))
          .filter(Boolean);

        const examples = extractExamples(rightPanel);
        const responseBodySection = examples.response_sections.find((section) => /^Body$/i.test(section.label))
          || examples.response_sections[0]
          || { text: '' };

        return {
          source_url: sourceUrl,
          source_anchor: `#${action.id}`,
          dom_id: clean(action.id),
          ordinal: index,
          section_heading: sectionHeading,
          title: clean(heading && heading.querySelector('.name') ? heading.querySelector('.name').innerText : ''),
          method: clean(heading && heading.querySelector('.method') ? heading.querySelector('.method').innerText : ''),
          path: clean(heading && heading.querySelector('code.uri') ? heading.querySelector('code.uri').innerText : ''),
          description: clean(directParagraphs.join('\\n\\n')),
          params: extractParams(action),
          request_sections: examples.request_sections,
          request_label: examples.request_label,
          response_sections: examples.response_sections,
          response_label: examples.response_label,
          response_body: clean(responseBodySection.text),
        };
      });
    }
    """

    async def _load_page(self, page: Page, url: str) -> None:
        logging.info("Parsing %s", url)
        await page.goto(url, wait_until="networkidle", timeout=REQUEST_TIMEOUT_MS)
        await page.wait_for_timeout(500)

    async def parse_url(self, browser: Browser, url: str) -> list[EndpointChunk]:
        context = await browser.new_context(ignore_https_errors=True, viewport={"width": 1600, "height": 1200})
        page = await context.new_page()
        try:
            await self._load_page(page, url)
            raw_endpoints = await page.evaluate(self.EXTRACT_SCRIPT, url)
        finally:
            await context.close()

        chunks: list[EndpointChunk] = []
        for raw in raw_endpoints:
            method = normalize_whitespace(raw.get("method")).upper()
            path = normalize_whitespace(raw.get("path"))
            summary = normalize_whitespace(raw.get("title"))
            if not method or not path or not summary:
                logging.warning("Skipping invalid endpoint on %s with id=%s", url, raw.get("dom_id"))
                continue

            section_heading = normalize_whitespace(raw.get("section_heading"))
            tags = choose_tags(section_heading, path)
            chunk = EndpointChunk(
                id="",
                method=method,
                path=path,
                summary=summary,
                description="",
                params=[],
                example_request="",
                example_response=normalize_response(raw.get("response_body")),
                tags=tags,
            )

            seen_params: set[tuple[str, str, str | None]] = set()
            for raw_param in raw.get("params") or []:
                param = normalize_param(raw_param)
                if not param:
                    continue
                key = (param.name, param.source, param.type)
                if key in seen_params:
                    continue
                seen_params.add(key)
                chunk.params.append(asdict(param))

            raw_request_example = format_request_example(raw.get("request_sections") or [])
            chunk.example_request = synthesize_example_request(chunk.method, chunk.path, chunk.params, raw_request_example)
            chunk.description = synthesize_description(
                chunk.summary,
                chunk.method,
                chunk.path,
                chunk.params,
                normalize_whitespace(raw.get("description")),
            )
            chunk.id = build_endpoint_id(chunk.method, chunk.path, chunk.summary, chunk.tags)
            chunks.append(chunk)

        logging.info("Parsed %s endpoint candidate(s) from %s", len(chunks), url)
        return chunks


class RecruiteeWriter:
    def __init__(self, output_root: Path, chunks_dir: Path) -> None:
        self.output_root = output_root
        self.chunks_dir = chunks_dir

    def prepare_output_dir(self) -> None:
        self.chunks_dir.mkdir(parents=True, exist_ok=True)
        for json_file in self.chunks_dir.glob("*.json"):
            json_file.unlink()
        for filename in ("index.json", "intents.json"):
            output_file = self.output_root / filename
            if output_file.exists():
                output_file.unlink()

    def _write_json(self, path: Path, payload: Any) -> None:
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=False) + "\n", encoding="utf-8")

    def write(self, endpoints: list[EndpointChunk]) -> None:
        self.prepare_output_dir()

        index: dict[str, list[str]] = defaultdict(list)
        intents: dict[str, str] = {}

        for endpoint in endpoints:
            payload = {
                "id": endpoint.id,
                "method": endpoint.method,
                "path": endpoint.path,
                "summary": endpoint.summary,
                "description": endpoint.description,
                "params": endpoint.params,
                "example_request": endpoint.example_request,
                "example_response": endpoint.example_response,
                "tags": endpoint.tags,
            }
            self._write_json(self.chunks_dir / f"{endpoint.id}.json", payload)

            for tag in endpoint.tags:
                index[tag].append(endpoint.id)

            intent_name = build_intent_name(endpoint)
            unique_intent = intent_name
            suffix = 2
            while unique_intent in intents and intents[unique_intent] != endpoint.id:
                unique_intent = f"{intent_name}_{suffix}"
                suffix += 1
            intents[unique_intent] = endpoint.id

        for tag, endpoint_ids in index.items():
            index[tag] = sorted(dict.fromkeys(endpoint_ids))

        self._write_json(self.output_root / "index.json", dict(sorted(index.items())))
        self._write_json(self.output_root / "intents.json", dict(sorted(intents.items())))
        logging.info("Wrote %s chunk file(s) to %s", len(endpoints), self.output_root.resolve())


def deduplicate_endpoints(endpoints: list[EndpointChunk]) -> list[EndpointChunk]:
    by_signature: dict[tuple[str, str], EndpointChunk] = {}
    for endpoint in endpoints:
        signature = (endpoint.method, endpoint.path)
        current = by_signature.get(signature)
        if current is None:
            by_signature[signature] = endpoint
            continue

        current_score = len(current.description) + len(current.params) * 10 + len(current.example_request)
        candidate_score = len(endpoint.description) + len(endpoint.params) * 10 + len(endpoint.example_request)
        if candidate_score > current_score:
            by_signature[signature] = endpoint

    unique = list(by_signature.values())
    used_ids: dict[str, int] = defaultdict(int)
    for endpoint in unique:
        used_ids[endpoint.id] += 1
        if used_ids[endpoint.id] > 1:
            endpoint.id = f"{endpoint.id}_{used_ids[endpoint.id]}"

    logging.info("Retained %s unique endpoint(s) after deduplication", len(unique))
    return sorted(unique, key=lambda item: (item.tags[0] if item.tags else "", item.path, item.method))


async def launch_browser() -> Browser:
    playwright = await async_playwright().start()
    try:
        try:
            browser = await playwright.chromium.launch(headless=True)
        except PlaywrightError as exc:
            if "Executable doesn't exist" not in str(exc):
                raise
            logging.info("Chromium is not installed. Installing Playwright browser bundle.")
            subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
            browser = await playwright.chromium.launch(headless=True)
        browser._recruitee_playwright = playwright  # type: ignore[attr-defined]
        return browser
    except Exception:
        await playwright.stop()
        raise


async def close_browser(browser: Browser) -> None:
    playwright = getattr(browser, "_recruitee_playwright", None)
    try:
        await browser.close()
    finally:
        if playwright is not None:
            await playwright.stop()


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    crawler = RecruiteeCrawler(BASE_URL)
    parser = RecruiteeParser()
    writer = RecruiteeWriter(OUTPUT_ROOT, CHUNKS_DIR)

    browser = await launch_browser()
    try:
        urls = await crawler.crawl(browser)
        parsed_endpoints: list[EndpointChunk] = []
        for url in urls:
            parsed_endpoints.extend(await parser.parse_url(browser, url))

        unique_endpoints = deduplicate_endpoints(parsed_endpoints)
        writer.write(unique_endpoints)
    finally:
        await close_browser(browser)


if __name__ == "__main__":
    asyncio.run(main())
