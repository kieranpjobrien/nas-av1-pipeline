"""A dead usenet subscription must announce itself, not retry in silence.

2026-08-15: news.newsgroupdirect.com returned "502 Access Denied. Please check
your login/pw." and eu-tst.newsgroupdirect.com "502 Connection failure",
1,586 times in twenty minutes. SAB reported healthy throughout, and the only
reason anyone noticed was the operator asking whether they were paying for
nothing.

The distinction that matters: a missing ARTICLE is ordinary retention loss and
means nothing about the subscription. A failed LOGIN means nobody is being
served at all.
"""

import collections

from tools.usenet_server_health import fatal_errors, group_by_account, volume_for


class TestFatalErrors:
    def test_failed_login_is_fatal(self):
        log = "WARNING::[downloader:911] Failed login for server news.newsgroupdirect.com [502 Access Denied.]"
        assert fatal_errors(log)["news.newsgroupdirect.com"] == 1

    def test_cannot_connect_is_fatal(self):
        log = "WARNING::[downloader:911] Cannot connect to server eu-tst.newsgroupdirect.com [502 Connection failure.]"
        assert fatal_errors(log)["eu-tst.newsgroupdirect.com"] == 1

    def test_missing_article_is_NOT_fatal(self):
        """Ordinary retention loss - counting it would condemn healthy servers."""
        log = "INFO::[article:192] Article abc@xyz unavailable on all servers, discarding"
        assert not fatal_errors(log)

    def test_successful_connection_is_not_fatal(self):
        log = "INFO::[newswrapper:632] 62@eu-tst.newsgroupdirect.com: Connected using TLSv1.3"
        assert not fatal_errors(log)

    def test_counts_repeats_per_host(self):
        log = "\n".join(
            ["Failed login for server a.example.com [502]"] * 3 + ["Cannot connect to server b.example.com [502]"]
        )
        e = fatal_errors(log)
        assert e["a.example.com"] == 3
        assert e["b.example.com"] == 1

    def test_empty_log_is_not_an_accusation(self):
        assert fatal_errors("") == collections.Counter()


class TestGroupByAccount:
    def test_shared_username_is_one_subscription(self):
        """viper/news/eu-tst.newsgroupdirect.com were all pqy542681482 - one
        bill. Judging per host let a working endpoint mask two dead ones."""
        servers = [
            {"host": "news.newsgroupdirect.com", "username": "pqy542681482"},
            {"host": "eu-tst.newsgroupdirect.com", "username": "pqy542681482"},
            {"host": "viper.newsgroupdirect.com", "username": "pqy542681482"},
            {"host": "aunews.frugalusenet.com", "username": "kieran"},
        ]
        g = group_by_account(servers)
        assert len(g) == 2
        assert len(g["pqy542681482"]) == 3
        assert len(g["kieran"]) == 1

    def test_missing_username_does_not_crash(self):
        assert "(none)" in group_by_account([{"host": "h", "username": None}])


class TestVolumeLookup:
    """Newshosting is configured as news.newshosting.com but SAB records its
    traffic under news.eweka.nl - it resells Eweka. Reporting 0.0 for it
    accused a server doing 9,579 GB/month of 'paying for nothing'."""

    STATS = {"news.eweka.nl": {"month": 9_579_000_000_000}, "aunews.frugalusenet.com": {"month": 14_000_000_000}}

    def test_exact_host_match(self):
        assert volume_for("aunews.frugalusenet.com", self.STATS) == 14.0

    def test_unmatched_host_returns_none_not_zero(self):
        """The whole point: 'cannot tell' must not read as 'earning nothing'."""
        assert volume_for("news.newshosting.com", self.STATS) is None

    def test_domain_fallback_matches_alias_within_a_domain(self):
        stats = {"eu-tst.newsgroupdirect.com": {"month": 5_000_000_000}}
        assert volume_for("news.newsgroupdirect.com", stats) == 5.0

    def test_missing_month_key_is_zero_not_crash(self):
        assert volume_for("h.example.com", {"h.example.com": {}}) == 0.0
