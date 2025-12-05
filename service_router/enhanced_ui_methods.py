# Enhanced UI Methods - to be integrated into EnhancedUI class in router.py
# This file contains the curses implementation methods for the nested menu system

def _main(self, stdscr):
    """Main curses loop with nested menu system."""
    curses.curs_set(0)
    stdscr.nodelay(True)
    stdscr.timeout(120)

    # Initialize colors
    if curses.has_colors():
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_CYAN, -1)  # Header
        curses.init_pair(2, curses.COLOR_GREEN, -1)  # Active/OK
        curses.init_pair(3, curses.COLOR_YELLOW, -1)  # Warning
        curses.init_pair(4, curses.COLOR_RED, -1)  # Error
        curses.init_pair(5, curses.COLOR_MAGENTA, -1)  # Section
        curses.init_pair(6, curses.COLOR_WHITE, curses.COLOR_BLUE)  # Selected

    while not self.stop.is_set():
        # Clear events queue
        try:
            while True:
                _ = self.events.get_nowait()
        except queue.Empty:
            pass

        stdscr.erase()
        h, w = stdscr.getmaxyx()

        if self.current_view == "main":
            self._render_main_menu(stdscr, h, w)
        elif self.current_view == "config":
            self._render_config_view(stdscr, h, w)
        elif self.current_view == "stats":
            self._render_stats_view(stdscr, h, w)
        elif self.current_view == "addressbook":
            self._render_address_book_view(stdscr, h, w)
        elif self.current_view == "ingress":
            self._render_ingress_view(stdscr, h, w)
        elif self.current_view == "egress":
            self._render_egress_view(stdscr, h, w)

        stdscr.refresh()

        # Handle input
        try:
            ch = stdscr.getch()
            self._handle_input(ch)
        except Exception:
            pass

def _render_main_menu(self, stdscr, h, w):
    """Render the main menu."""
    # Header
    header = "═══ Hydra NKN Router ═══"
    self._safe_addstr(stdscr, 0, (w - len(header)) // 2, header, curses.color_pair(1) | curses.A_BOLD)

    # Help line
    help_text = "↑/↓: Navigate | Enter: Select | Q: Quit"
    self._safe_addstr(stdscr, 1, (w - len(help_text)) // 2, help_text, curses.A_DIM)

    # Menu items
    start_row = 4
    for i, item in enumerate(self.MENU_ITEMS):
        row = start_row + i * 2
        if row >= h - 2:
            break

        if i == self.main_menu_index:
            # Selected item
            text = f"  ▶ {item} ◀"
            attr = curses.color_pair(6) | curses.A_BOLD
        else:
            text = f"    {item}    "
            attr = curses.color_pair(2)

        col = (w - len(text)) // 2
        self._safe_addstr(stdscr, row, col, text, attr)

    # Status footer
    status = f"Services: {len(self.services)} | Nodes: {len(self.nodes)}"
    self._safe_addstr(stdscr, h - 1, 2, status, curses.A_DIM)

def _render_config_view(self, stdscr, h, w):
    """Render the Config view with service enable/disable."""
    # Header with border
    self._draw_box(stdscr, 0, 0, h, w)
    title = "═══ Configuration ═══"
    self._safe_addstr(stdscr, 0, (w - len(title)) // 2, title, curses.color_pair(1) | curses.A_BOLD)

    # Help
    help_text = "↑/↓: Navigate | Space: Toggle | S: Save | ESC: Back"
    self._safe_addstr(stdscr, 1, 2, help_text, curses.A_DIM)

    # Service list
    start_row = 3
    services = sorted(self.services.keys())

    if not services:
        self._safe_addstr(stdscr, start_row, 2, "(No services configured)", curses.A_DIM)
        return

    self._safe_addstr(stdscr, start_row, 2, "Services:", curses.color_pair(5) | curses.A_BOLD)
    start_row += 2

    visible_rows = h - start_row - 2
    end_idx = min(len(services), self.scroll_offset + visible_rows)

    for i in range(self.scroll_offset, end_idx):
        svc = services[i]
        enabled = self.service_config.get(svc, True)
        status = "[✓]" if enabled else "[ ]"

        row = start_row + (i - self.scroll_offset)
        if i == self.main_menu_index:
            # Selected
            text = f"▶ {status} {svc}"
            attr = curses.color_pair(6)
        else:
            text = f"  {status} {svc}"
            attr = curses.color_pair(2) if enabled else curses.A_DIM

        self._safe_addstr(stdscr, row, 4, text, attr)

        # Service info
        info = self.services.get(svc, {})
        addr = info.get("assigned_addr", "—")
        if len(addr) > 20:
            addr = addr[:17] + "..."
        state = info.get("status", "unknown")
        detail = f"{state} | {addr}"
        self._safe_addstr(stdscr, row, 40, detail, curses.A_DIM)

def _render_stats_view(self, stdscr, h, w):
    """Render Statistics view with ASCII bar graphs."""
    self._draw_box(stdscr, 0, 0, h, w)
    title = "═══ Service Statistics (24h) ═══"
    self._safe_addstr(stdscr, 0, (w - len(title)) // 2, title, curses.color_pair(1) | curses.A_BOLD)

    help_text = "ESC: Back"
    self._safe_addstr(stdscr, 1, 2, help_text, curses.A_DIM)

    # Get service timeline
    timeline = self.stats.get_service_timeline(24)

    if not timeline:
        self._safe_addstr(stdscr, 3, 2, "(No activity in last 24 hours)", curses.A_DIM)
        return

    # Render timeline with ASCII bars
    start_row = 3
    row = start_row

    for svc in sorted(timeline.keys()):
        if row >= h - 2:
            break

        history = timeline[svc]
        if not history:
            continue

        # Aggregate into hourly buckets
        buckets = [0] * 24
        now = time.time()
        for ts, count in history:
            hours_ago = int((now - ts) / 3600)
            if 0 <= hours_ago < 24:
                buckets[23 - hours_ago] += count

        # Service name
        svc_label = (svc[:15] + "...") if len(svc) > 18 else svc
        self._safe_addstr(stdscr, row, 2, f"{svc_label:18}", curses.color_pair(5))

        # ASCII bar graph
        max_val = max(buckets) if max(buckets) > 0 else 1
        bar_width = min(40, w - 25)

        for i, count in enumerate(buckets[-bar_width:]):
            if count > 0:
                height = int((count / max_val) * 5) + 1
                bar_char = "▁▂▃▄▅▆▇█"[min(height - 1, 7)]
                color = curses.color_pair(2) if count > 0 else curses.A_DIM
                self._safe_addstr(stdscr, row, 22 + i, bar_char, color)

        # Total count
        total = sum(buckets)
        self._safe_addstr(stdscr, row, w - 12, f"({total:>5})", curses.A_DIM)

        row += 1

    # Time axis labels
    if row < h - 1:
        self._safe_addstr(stdscr, row + 1, 22, "←24h", curses.A_DIM)
        self._safe_addstr(stdscr, row + 1, w - 8, "now→", curses.A_DIM)

def _render_address_book_view(stdscr, h, w):
    """Render Address Book with NKN addresses and usage stats."""
    self._draw_box(stdscr, 0, 0, h, w)
    title = "═══ Address Book ═══"
    self._safe_addstr(stdscr, 0, (w - len(title)) // 2, title, curses.color_pair(1) | curses.A_BOLD)

    help_text = "↑/↓: Navigate | Enter: Details | ESC: Back"
    self._safe_addstr(stdscr, 1, 2, help_text, curses.A_DIM)

    # Get addresses
    addresses = self.stats.get_address_book()

    if not addresses:
        self._safe_addstr(stdscr, 3, 2, "(No visitors yet)", curses.A_DIM)
        return

    # Table header
    start_row = 3
    header = f"{'Address':<45} {'Requests':>10} {'Last Seen':>20}"
    self._safe_addstr(stdscr, start_row, 2, header, curses.color_pair(5) | curses.A_BOLD)
    start_row += 1
    self._safe_addstr(stdscr, start_row, 2, "─" * (w - 4), curses.A_DIM)
    start_row += 1

    # Address list
    visible_rows = h - start_row - 2
    end_idx = min(len(addresses), self.scroll_offset + visible_rows)

    for i in range(self.scroll_offset, end_idx):
        addr_info = addresses[i]
        addr = addr_info["addr"]
        total = addr_info.get("total_requests", 0)
        last_seen = addr_info.get("last_seen", 0)
        last_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_seen))

        # Truncate address if too long
        addr_display = (addr[:42] + "...") if len(addr) > 45 else addr

        row = start_row + (i - self.scroll_offset)
        if i == self.main_menu_index:
            attr = curses.color_pair(6)
            prefix = "▶ "
        else:
            attr = curses.A_NORMAL
            prefix = "  "

        line = f"{prefix}{addr_display:<45} {total:>10} {last_str:>20}"
        self._safe_addstr(stdscr, row, 2, line, attr)

def _render_ingress_view(self, stdscr, h, w):
    """Render Ingress view with QR codes for service addresses."""
    self._draw_box(stdscr, 0, 0, h, w)
    title = "═══ Ingress Addresses ═══"
    self._safe_addstr(stdscr, 0, (w - len(title)) // 2, title, curses.color_pair(1) | curses.A_BOLD)

    help_text = "↑/↓: Navigate | Enter: Show QR | ESC: Back"
    self._safe_addstr(stdscr, 1, 2, help_text, curses.A_DIM)

    if self.show_qr and self.qr_data:
        # Show QR code
        self._render_qr_code(stdscr, 3, 2, h - 4, w - 4)
        return

    # Service list
    start_row = 3
    services = sorted(self.services.keys())

    if not services:
        self._safe_addstr(stdscr, start_row, 2, "(No services available)", curses.A_DIM)
        return

    visible_rows = h - start_row - 2
    end_idx = min(len(services), self.scroll_offset + visible_rows)

    for i in range(self.scroll_offset, end_idx):
        svc = services[i]
        info = self.services.get(svc, {})
        addr = info.get("assigned_addr", "—")
        state = info.get("status", "unknown")

        row = start_row + (i - self.scroll_offset)
        if i == self.main_menu_index:
            attr = curses.color_pair(6)
            prefix = "▶ "
        else:
            attr = curses.color_pair(2) if state == "ready" else curses.A_DIM
            prefix = "  "

        svc_display = (svc[:20] + "...") if len(svc) > 23 else svc
        addr_display = (addr[:40] + "...") if len(addr) > 43 else addr

        line = f"{prefix}{svc_display:<23} [{state:<8}] {addr_display}"
        self._safe_addstr(stdscr, row, 2, line, attr)

def _render_egress_view(self, stdscr, h, w):
    """Render Egress view with bandwidth and user leaderboard."""
    self._draw_box(stdscr, 0, 0, h, w)
    title = "═══ Egress Statistics ═══"
    self._safe_addstr(stdscr, 0, (w - len(title)) // 2, title, curses.color_pair(1) | curses.A_BOLD)

    help_text = "ESC: Back"
    self._safe_addstr(stdscr, 1, 2, help_text, curses.A_DIM)

    # Get egress stats
    egress = self.stats.get_egress_stats()

    if not egress:
        self._safe_addstr(stdscr, 3, 2, "(No egress data)", curses.A_DIM)
        return

    # Service summary
    start_row = 3
    self._safe_addstr(stdscr, start_row, 2, "Service Summary:", curses.color_pair(5) | curses.A_BOLD)
    start_row += 2

    header = f"{'Service':<20} {'Requests':>10} {'Bandwidth':>15} {'Users':>8}"
    self._safe_addstr(stdscr, start_row, 2, header, curses.A_BOLD)
    start_row += 1

    for svc in sorted(egress.keys()):
        if start_row >= h // 2:
            break

        stats = egress[svc]
        req_count = stats.get("request_count", 0)
        bytes_sent = stats.get("bytes_sent", 0)
        users = len(stats.get("users", {}))

        # Format bandwidth
        if bytes_sent > 1024 * 1024 * 1024:
            bw = f"{bytes_sent / (1024**3):.2f} GB"
        elif bytes_sent > 1024 * 1024:
            bw = f"{bytes_sent / (1024**2):.2f} MB"
        elif bytes_sent > 1024:
            bw = f"{bytes_sent / 1024:.2f} KB"
        else:
            bw = f"{bytes_sent} B"

        svc_display = (svc[:17] + "...") if len(svc) > 20 else svc
        line = f"  {svc_display:<20} {req_count:>10} {bw:>15} {users:>8}"
        self._safe_addstr(stdscr, start_row, 2, line, curses.color_pair(2))
        start_row += 1

    # User leaderboard
    start_row += 2
    if start_row < h - 5:
        self._safe_addstr(stdscr, start_row, 2, "Top Users (by bandwidth):", curses.color_pair(5) | curses.A_BOLD)
        start_row += 2

        # Aggregate users across all services
        user_totals = {}
        for stats in egress.values():
            for user, bytes_sent in stats.get("users", {}).items():
                user_totals[user] = user_totals.get(user, 0) + bytes_sent

        # Sort by bandwidth
        top_users = sorted(user_totals.items(), key=lambda x: x[1], reverse=True)[:10]

        for i, (user, bytes_sent) in enumerate(top_users):
            if start_row >= h - 2:
                break

            # Format bandwidth
            if bytes_sent > 1024 * 1024 * 1024:
                bw = f"{bytes_sent / (1024**3):.2f} GB"
            elif bytes_sent > 1024 * 1024:
                bw = f"{bytes_sent / (1024**2):.2f} MB"
            else:
                bw = f"{bytes_sent / 1024:.2f} KB"

            user_display = (user[:40] + "...") if len(user) > 43 else user
            line = f"  {i+1:2}. {user_display:<43} {bw:>15}"
            self._safe_addstr(stdscr, start_row, 2, line, curses.A_NORMAL)
            start_row += 1

def _render_qr_code(self, stdscr, y, x, max_h, max_w):
    """Render QR code for selected service."""
    if not self.qr_data:
        return

    # Generate QR code
    qr_text = render_qr_ascii(self.qr_data, scale=1, invert=False)
    lines = qr_text.splitlines()

    # Center QR code
    qr_h = len(lines)
    qr_w = max(len(line) for line in lines) if lines else 0

    start_y = y + (max_h - qr_h) // 2
    start_x = x + (max_w - qr_w) // 2

    # Label
    if self.qr_label:
        label_y = max(0, start_y - 2)
        self._safe_addstr(stdscr, label_y, start_x, self.qr_label, curses.color_pair(1) | curses.A_BOLD)

    # QR code
    for i, line in enumerate(lines):
        row = start_y + i
        if row >= y + max_h:
            break
        self._safe_addstr(stdscr, row, start_x, line, curses.A_NORMAL)

    # Instructions
    inst_y = start_y + qr_h + 2
    if inst_y < y + max_h:
        self._safe_addstr(stdscr, inst_y, start_x, "Press ESC to close", curses.A_DIM)

def _handle_input(self, ch):
    """Handle keyboard input for navigation."""
    if ch == ord('q') or ch == ord('Q'):
        if self.current_view == "main":
            self.stop.set()
        else:
            self.current_view = "main"
            self.main_menu_index = 0
            self.scroll_offset = 0
            self.show_qr = False

    elif ch == 27:  # ESC
        if self.show_qr:
            self.show_qr = False
        elif self.current_view != "main":
            self.current_view = "main"
            self.main_menu_index = 0
            self.scroll_offset = 0
        else:
            self.stop.set()

    elif ch in (curses.KEY_UP, ord('k')):
        if self.current_view == "main":
            self.main_menu_index = (self.main_menu_index - 1) % len(self.MENU_ITEMS)
        else:
            self.main_menu_index = max(0, self.main_menu_index - 1)
            # Adjust scroll
            if self.main_menu_index < self.scroll_offset:
                self.scroll_offset = self.main_menu_index

    elif ch in (curses.KEY_DOWN, ord('j')):
        if self.current_view == "main":
            self.main_menu_index = (self.main_menu_index + 1) % len(self.MENU_ITEMS)
        else:
            # Get max index based on current view
            max_idx = 0
            if self.current_view == "config":
                max_idx = max(0, len(self.services) - 1)
            elif self.current_view == "addressbook":
                max_idx = max(0, len(self.stats.get_address_book()) - 1)
            elif self.current_view == "ingress":
                max_idx = max(0, len(self.services) - 1)

            self.main_menu_index = min(max_idx, self.main_menu_index + 1)

    elif ch in (curses.KEY_ENTER, 10, 13):
        self._handle_enter()

    elif ch == ord(' '):
        if self.current_view == "config":
            # Toggle service enabled/disabled
            services = sorted(self.services.keys())
            if 0 <= self.main_menu_index < len(services):
                svc = services[self.main_menu_index]
                self.service_config[svc] = not self.service_config.get(svc, True)

    elif ch in (ord('s'), ord('S')):
        if self.current_view == "config":
            self._save_service_config()

def _handle_enter(self):
    """Handle Enter key based on current view."""
    if self.current_view == "main":
        # Navigate to selected menu item
        selected = self.MENU_ITEMS[self.main_menu_index]
        if selected == "Config":
            self.current_view = "config"
        elif selected == "Statistics":
            self.current_view = "stats"
        elif selected == "Address Book":
            self.current_view = "addressbook"
        elif selected == "Ingress":
            self.current_view = "ingress"
        elif selected == "Egress":
            self.current_view = "egress"

        self.main_menu_index = 0
        self.scroll_offset = 0

    elif self.current_view == "ingress":
        # Show QR code for selected service
        services = sorted(self.services.keys())
        if 0 <= self.main_menu_index < len(services):
            svc = services[self.main_menu_index]
            info = self.services.get(svc, {})
            addr = info.get("assigned_addr", "")
            if addr and addr != "—":
                self.qr_data = addr
                self.qr_label = f"Service: {svc}"
                self.show_qr = True

# Helper methods
def _draw_box(self, stdscr, y, x, h, w):
    """Draw a box border."""
    try:
        stdscr.attron(curses.A_DIM)
        # Top and bottom
        stdscr.addstr(y, x, "╔" + "═" * (w - 2) + "╗")
        stdscr.addstr(y + h - 1, x, "╚" + "═" * (w - 2) + "╝")
        # Sides
        for row in range(y + 1, y + h - 1):
            stdscr.addstr(row, x, "║")
            stdscr.addstr(row, x + w - 1, "║")
        stdscr.attroff(curses.A_DIM)
    except curses.error:
        pass

def _safe_addstr(self, stdscr, y, x, text, attr=curses.A_NORMAL):
    """Safely add string to stdscr, handling errors."""
    try:
        h, w = stdscr.getmaxyx()
        if 0 <= y < h and 0 <= x < w:
            # Truncate text to fit
            max_len = w - x - 1
            if len(text) > max_len:
                text = text[:max_len]
            stdscr.addstr(y, x, text, attr)
    except curses.error:
        pass
