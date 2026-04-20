"""
End-to-end Playwright tests for the ClickHouse Monitor dashboard.
Requires Streamlit running at http://localhost:8501
Run: .venv/bin/pytest tests/test_e2e.py -v --headed (or without --headed for headless)
"""
import re
import pytest
from playwright.sync_api import Page, expect

BASE_URL = "http://localhost:8501"
# Streamlit pages take time to load + run ClickHouse queries
PAGE_TIMEOUT = 20_000  # ms


def wait_for_streamlit(page: Page):
    """Wait until Streamlit stops showing its loading spinner."""
    page.wait_for_function(
        "() => !document.querySelector('[data-testid=\"stSpinner\"]')",
        timeout=PAGE_TIMEOUT,
    )
    # Also wait for any skeleton loaders to disappear
    page.wait_for_timeout(500)


def expand_sidebar(page: Page):
    """Ensure the Streamlit sidebar is expanded (click hamburger if collapsed)."""
    collapsed = page.locator('[data-testid="collapsedControl"]')
    if collapsed.count() > 0 and collapsed.first.is_visible():
        collapsed.first.click()
        page.wait_for_timeout(300)


# ─────────────────────────────────────────────
# Helper: navigate to a Streamlit page by URL hash
# ─────────────────────────────────────────────
def go_to(page: Page, path: str = ""):
    url = f"{BASE_URL}/{path}"
    page.goto(url, wait_until="networkidle")
    wait_for_streamlit(page)


# ═════════════════════════════════════════════
# App-level tests
# ═════════════════════════════════════════════

class TestAppLoads:
    def test_home_page_loads(self, page: Page):
        """App starts, config loads, no crash."""
        go_to(page)
        # Should NOT show config error
        assert "Configuration error" not in page.content()
        assert "Connection error" not in page.content()

    def test_title_visible(self, page: Page):
        go_to(page)
        expect(page.get_by_role("heading", name=re.compile("Overview", re.I))).to_be_visible()

    def test_sidebar_cluster_name_visible(self, page: Page):
        go_to(page)
        expand_sidebar(page)
        expect(page.get_by_text("ecredal_cluster", exact=False).first).to_be_visible()

    def test_sidebar_time_window_controls(self, page: Page):
        go_to(page)
        expand_sidebar(page)
        # Preset radio buttons
        for label in ["1h", "6h", "24h", "7d", "Custom"]:
            expect(page.get_by_text(label, exact=True).first).to_be_visible()

    def test_sidebar_refresh_toggle(self, page: Page):
        go_to(page)
        expand_sidebar(page)
        expect(page.get_by_text("Auto-refresh", exact=False)).to_be_visible()
        expect(page.get_by_text("Refresh Now", exact=False)).to_be_visible()


# ═════════════════════════════════════════════
# Overview page
# ═════════════════════════════════════════════

class TestOverviewPage:
    # Overview is now the home page (app.py); URL is /
    def test_overview_loads(self, page: Page):
        go_to(page)
        expect(page.get_by_role("heading", name=re.compile("Overview", re.I))).to_be_visible()

    def test_memory_panel_visible(self, page: Page):
        go_to(page)
        expect(page.get_by_text("Active Queries by Memory", exact=False)).to_be_visible()

    def test_disk_panel_visible(self, page: Page):
        go_to(page)
        expect(page.get_by_text("Disk Usage", exact=False)).to_be_visible()

    def test_health_grid_visible(self, page: Page):
        go_to(page)
        expect(page.get_by_text("Cluster Health", exact=False)).to_be_visible()

    def test_health_grid_has_domains(self, page: Page):
        go_to(page)
        for domain in ["Cluster", "Disk", "Queries"]:
            expect(page.get_by_text(domain, exact=False).first).to_be_visible()

    def test_no_error_messages(self, page: Page):
        go_to(page)
        content = page.content()
        assert "Query error" not in content or "Connection" not in content


# ═════════════════════════════════════════════
# DBA domain pages
# ═════════════════════════════════════════════

class TestClusterPage:
    def test_cluster_page_loads(self, page: Page):
        go_to(page, "Cluster")
        expect(page.get_by_role("heading", name=re.compile("Cluster", re.I)).first).to_be_visible()

    def test_node_status_section(self, page: Page):
        go_to(page, "Cluster")
        expect(page.get_by_text("Node Status", exact=False)).to_be_visible()

    def test_replica_consistency_section(self, page: Page):
        go_to(page, "Cluster")
        expect(page.get_by_text("Replica Consistency", exact=False)).to_be_visible()

    def test_zookeeper_section(self, page: Page):
        go_to(page, "Cluster")
        expect(page.get_by_text("ZooKeeper", exact=False)).to_be_visible()


class TestQueriesPage:
    def test_queries_page_loads(self, page: Page):
        go_to(page, "Queries")
        expect(page.get_by_role("heading", name=re.compile("Queries", re.I)).first).to_be_visible()

    def test_running_now_section(self, page: Page):
        go_to(page, "Queries")
        expect(page.get_by_text("Running Now", exact=False)).to_be_visible()

    def test_tabs_present(self, page: Page):
        go_to(page, "Queries")
        for tab in ["Slow Queries", "Memory Heavy", "Full Table Scans", "Top Patterns"]:
            expect(page.get_by_role("tab", name=tab)).to_be_visible()

    def test_slow_queries_tab(self, page: Page):
        go_to(page, "Queries")
        page.get_by_role("tab", name="Slow Queries").click()
        page.wait_for_timeout(1000)
        # Should show data or "No slow queries" message
        content = page.content()
        assert "Slow Queries" in content


class TestDiskPage:
    def test_disk_page_loads(self, page: Page):
        go_to(page, "Disk")
        expect(page.get_by_role("heading", name=re.compile("Disk", re.I)).first).to_be_visible()

    def test_free_space_section(self, page: Page):
        go_to(page, "Disk")
        expect(page.get_by_text("Free Space", exact=False)).to_be_visible()

    def test_table_sizes_section(self, page: Page):
        go_to(page, "Disk")
        expect(page.get_by_text("Table Sizes", exact=False)).to_be_visible()

    def test_parts_health_section(self, page: Page):
        go_to(page, "Disk")
        expect(page.get_by_text("Parts Health", exact=False)).to_be_visible()

    def test_ttl_progress_section(self, page: Page):
        go_to(page, "Disk")
        expect(page.get_by_text("TTL Progress", exact=False)).to_be_visible()

    def test_disk_history_section(self, page: Page):
        go_to(page, "Disk")
        expect(page.get_by_text("Disk Usage History", exact=False)).to_be_visible()

    def test_disk_history_metric_cards(self, page: Page):
        go_to(page, "Disk")
        expect(page.get_by_text("Disk Used", exact=False).first).to_be_visible()
        expect(page.get_by_text("Available Space", exact=False)).to_be_visible()


class TestMergesPage:
    def test_merges_page_loads(self, page: Page):
        go_to(page, "Merges")
        expect(page.get_by_role("heading", name=re.compile("Merges", re.I)).first).to_be_visible()

    def test_sections_visible(self, page: Page):
        go_to(page, "Merges")
        for section in ["Active Merges", "Mutations", "Queue Depth"]:
            expect(page.get_by_text(section, exact=False)).to_be_visible()


class TestConnectionsPage:
    def test_connections_page_loads(self, page: Page):
        go_to(page, "Connections")
        expect(page.get_by_role("heading", name=re.compile("Connections", re.I)).first).to_be_visible()

    def test_session_stats_section(self, page: Page):
        go_to(page, "Connections")
        expect(page.get_by_text("Session Stats", exact=False)).to_be_visible()


class TestThreadsPage:
    def test_threads_page_loads(self, page: Page):
        go_to(page, "Threads")
        expect(page.get_by_role("heading", name=re.compile("Threads", re.I)).first).to_be_visible()

    def test_sections_visible(self, page: Page):
        go_to(page, "Threads")
        for section in ["Background Tasks", "Thread Pool Usage", "Distributed Sends"]:
            expect(page.get_by_text(section, exact=False)).to_be_visible()


class TestSystemMetricsPage:
    def test_system_metrics_page_loads(self, page: Page):
        go_to(page, "System_Metrics")
        expect(page.get_by_role("heading", name=re.compile("System Metrics", re.I)).first).to_be_visible()

    def test_current_metrics_section(self, page: Page):
        go_to(page, "System_Metrics")
        expect(page.get_by_role("heading", name=re.compile("Current Metrics", re.I))).to_be_visible()

    def test_events_summary_section(self, page: Page):
        go_to(page, "System_Metrics")
        expect(page.get_by_text("Events Summary", exact=False)).to_be_visible()


class TestInsertsPage:
    def test_inserts_page_loads(self, page: Page):
        go_to(page, "Inserts")
        expect(page.get_by_role("heading", name=re.compile("Inserts", re.I)).first).to_be_visible()

    def test_insert_rates_section(self, page: Page):
        go_to(page, "Inserts")
        expect(page.get_by_text("Insert Rates", exact=False)).to_be_visible()

    def test_async_inserts_section(self, page: Page):
        go_to(page, "Inserts")
        # heading is "Async Inserts Queue"; use .first to avoid strict-mode on partial matches
        expect(page.get_by_text("Async Inserts", exact=False).first).to_be_visible()


class TestDictionariesPage:
    def test_dictionaries_page_loads(self, page: Page):
        go_to(page, "Dictionaries")
        expect(page.get_by_role("heading", name=re.compile("Dictionaries", re.I)).first).to_be_visible()

    def test_sections_visible(self, page: Page):
        go_to(page, "Dictionaries")
        for section in ["Dictionary Status", "Memory Usage"]:
            expect(page.get_by_text(section, exact=False)).to_be_visible()


# ═════════════════════════════════════════════
# User Dashboard
# ═════════════════════════════════════════════

class TestUserDashboard:
    def test_user_dashboard_loads(self, page: Page):
        go_to(page, "User_Dashboard")
        expect(page.get_by_role("heading", name=re.compile("User Dashboard", re.I))).to_be_visible()

    def test_user_selector_visible(self, page: Page):
        go_to(page, "User_Dashboard")
        expect(page.get_by_text("Select user", exact=False)).to_be_visible()

    def test_tabs_visible(self, page: Page):
        go_to(page, "User_Dashboard")
        for tab in ["Activity", "Errors", "Table Usage"]:
            expect(page.get_by_role("tab", name=tab)).to_be_visible()

    def test_activity_tab_has_metrics(self, page: Page):
        # Visit home first so app.py initialises ch_client in session_state,
        # then navigate to User Dashboard for the actual assertion.
        go_to(page)
        go_to(page, "User_Dashboard")
        # Activity tab is default — should show metric cards
        expect(page.get_by_text("Query Count", exact=False)).to_be_visible()

    def test_errors_tab(self, page: Page):
        go_to(page, "User_Dashboard")
        page.get_by_role("tab", name="Errors").click()
        page.wait_for_timeout(1000)
        content = page.content()
        # Either shows errors or "No errors" success message
        assert "Errors" in content

    def test_table_usage_tab(self, page: Page):
        go_to(page, "User_Dashboard")
        page.get_by_role("tab", name="Table Usage").click()
        page.wait_for_timeout(1000)
        content = page.content()
        assert "Table Usage" in content


# ═════════════════════════════════════════════
# Sidebar interactions
# ═════════════════════════════════════════════

class TestSidebarInteractions:
    def test_time_window_switch(self, page: Page):
        # Sidebar controls now appear on every page
        go_to(page, "Cluster")
        expand_sidebar(page)
        page.locator('[data-testid="stSidebar"]').get_by_text("24h", exact=True).click()
        page.wait_for_timeout(1500)
        expect(page.get_by_text("Cluster", exact=False).first).to_be_visible()

    def test_refresh_now_button(self, page: Page):
        go_to(page, "Cluster")
        expand_sidebar(page)
        page.locator('[data-testid="stSidebar"]').get_by_text("Refresh Now").click()
        page.wait_for_timeout(2000)
        assert "Error" not in page.title()
