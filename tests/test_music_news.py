"""music_news.py -- the Threads trending-news fallback content source for a
slow news day (see test_threads_post.py's own _post_music_news_fallback
tests for how this plugs into post_trending_news)."""
from __future__ import annotations

import xml.etree.ElementTree as ET

from data import music_news as news


def _rich_item(title, source, image_url="https://x.com/artist.jpg"):
    return {"title": title, "link": f"https://{source}.example/a", "image_url": image_url, "source": source}


def test_get_trending_story_prefers_a_picture_bearing_item(monkeypatch):
    def fake_fetch(url, *, source_name, limit=15):
        return {
            "Pitchfork": [_rich_item("No photo on this one", "Pitchfork", image_url=None)],
            "NME": [_rich_item("Artist announces new album", "NME")],
            "Billboard": [],
        }.get(source_name, [])

    monkeypatch.setattr(news, "_fetch_rss_items", fake_fetch)
    story = news.get_trending_story()
    assert story is not None
    assert story["title"] == "Artist announces new album"
    assert story["image_url"] == "https://x.com/artist.jpg"


def test_get_trending_story_falls_back_to_freshest_item_with_no_photo_at_all(monkeypatch):
    def fake_fetch(url, *, source_name, limit=15):
        return {
            "Pitchfork": [_rich_item("Freshest story, no photo", "Pitchfork", image_url=None)],
            "NME": [_rich_item("Second story, also no photo", "NME", image_url=None)],
            "Billboard": [],
        }.get(source_name, [])

    monkeypatch.setattr(news, "_fetch_rss_items", fake_fetch)
    story = news.get_trending_story()
    assert story is not None
    assert story["title"] == "Freshest story, no photo"
    assert story["image_url"] is None


def test_get_trending_story_returns_none_when_every_feed_fails(monkeypatch):
    monkeypatch.setattr(news, "_fetch_rss_items", lambda url, *, source_name, limit=15: [])
    assert news.get_trending_story() is None


def test_get_trending_story_secondary_excludes_the_lead_and_caps_at_three(monkeypatch):
    def fake_fetch(url, *, source_name, limit=15):
        if source_name != "Pitchfork":
            return []
        return [_rich_item(f"Story {i}", "Pitchfork") for i in range(6)]

    monkeypatch.setattr(news, "_fetch_rss_items", fake_fetch)
    story = news.get_trending_story()
    assert story is not None
    assert story["title"] not in story["secondary"]
    assert len(story["secondary"]) == 3


def test_get_trending_story_skips_an_excluded_picture_item_for_the_next_fresh_one(monkeypatch):
    """Real, confirmed bug this closes: the old version always took the
    single freshest picture-bearing item as the lead and gave up entirely
    if it was a recent duplicate -- even with a second fresh photo item
    right there in the same fetch. `exclude` walks past a stale lead
    instead of surfacing "nothing notable" every cycle a slow music news
    day repeats the same top story."""
    def fake_fetch(url, *, source_name, limit=15):
        return {
            "Pitchfork": [_rich_item("Artist announces new album", "Pitchfork")],
            "NME": [_rich_item("A different band drops surprise single", "NME")],
            "Billboard": [],
        }.get(source_name, [])

    monkeypatch.setattr(news, "_fetch_rss_items", fake_fetch)
    story = news.get_trending_story(exclude=lambda title: title == "Artist announces new album")
    assert story is not None
    assert story["title"] == "A different band drops surprise single"


def test_get_trending_story_returns_none_when_every_item_is_excluded(monkeypatch):
    def fake_fetch(url, *, source_name, limit=15):
        if source_name != "Pitchfork":
            return []
        return [_rich_item("Only story available", "Pitchfork")]

    monkeypatch.setattr(news, "_fetch_rss_items", fake_fetch)
    assert news.get_trending_story(exclude=lambda title: True) is None


def _item_xml(inner: str) -> ET.Element:
    return ET.fromstring(f"<item>{inner}</item>")


def test_extract_image_url_reads_a_media_thumbnail_tag():
    item = _item_xml(
        '<media:thumbnail xmlns:media="http://search.yahoo.com/mrss/" url="https://x.com/photo.jpg"/>'
    )
    assert news._extract_image_url(item) == "https://x.com/photo.jpg"  # noqa: SLF001


def test_extract_image_url_reads_an_enclosure_tag():
    item = _item_xml('<enclosure url="https://x.com/enc.jpg" type="image/jpeg"/>')
    assert news._extract_image_url(item) == "https://x.com/enc.jpg"  # noqa: SLF001


def test_extract_image_url_falls_back_to_an_img_tag_in_content_encoded():
    item = _item_xml(
        '<content:encoded xmlns:content="http://purl.org/rss/1.0/modules/content/">'
        '&lt;p&gt;&lt;img src="https://x.com/from-body.jpg" alt=""&gt;&lt;/p&gt;'
        "</content:encoded>"
    )
    assert news._extract_image_url(item) == "https://x.com/from-body.jpg"  # noqa: SLF001


def test_extract_image_url_falls_back_to_an_img_tag_in_description():
    item = _item_xml('<description>&lt;img src="https://x.com/desc.jpg"&gt;</description>')
    assert news._extract_image_url(item) == "https://x.com/desc.jpg"  # noqa: SLF001


def test_extract_image_url_returns_none_when_nothing_is_found():
    item = _item_xml("<title>No image anywhere</title>")
    assert news._extract_image_url(item) is None  # noqa: SLF001


def test_fetch_rss_items_survives_a_network_failure(monkeypatch):
    def raise_error(*a, **k):
        raise RuntimeError("simulated network failure")

    monkeypatch.setattr(news, "_rich_feed_cache", {})
    monkeypatch.setattr(news.requests, "get", raise_error)
    assert news._fetch_rss_items("https://example.com/feed", source_name="Test") == []  # noqa: SLF001
