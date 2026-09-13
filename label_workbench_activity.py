"""Session and deferred activity pane construction."""


def create_activity_pane(
    self,
    *,
    profile,
    panes,
    tk,
    ttk,
    DEFERRED_OPERATOR_STATUS_GROUPS,
    DEFERRED_ALERT_THRESHOLDS,
):
    self.operator_right_pane = ttk.Frame(
        self.operator_workbench_frame,
        style="Card.TFrame",
        padding=profile["card_padding"],
    )
    self.operator_right_pane.grid(row=0, column=2, sticky="nsew")
    self.right_activity_card = self.operator_right_pane
    self.operator_right_pane.grid_rowconfigure(0, weight=1)
    self.operator_right_pane.grid_columnconfigure(0, weight=1)
    self.operator_notebook = ttk.Notebook(
        self.operator_right_pane, style="Operator.TNotebook"
    )
    self.operator_notebook.grid(row=0, column=0, sticky="nsew")
    self.operator_history_notebook = self.operator_notebook

    self.session_tab = ttk.Frame(self.operator_notebook, style="Card.TFrame", padding=8)
    self.operator_session_tab = self.session_tab
    self.session_tab.grid_rowconfigure(1, weight=1)
    self.session_tab.grid_columnconfigure(0, weight=1)
    self.operator_notebook.add(self.session_tab, text="이번 세션")
    self.operator_session_heading_label = ttk.Label(
        self.session_tab,
        text="최근 완료",
        style="Header.TLabel",
    )
    self.operator_session_heading_label.grid(
        row=0, column=0, sticky="w", pady=(0, 6)
    )
    self.session_tree = ttk.Treeview(
        self.session_tab,
        columns=("Time", "Item", "Result"),
        show="headings",
        selectmode="browse",
    )
    for column, text, width in (
        ("Time", "시각", 68),
        ("Item", "현품표", 185),
        ("Result", "결과", 72),
    ):
        self.session_tree.heading(column, text=text, anchor="center")
        self.session_tree.column(column, width=width, minwidth=55, stretch=(column == "Item"), anchor="center")
    self.session_tree.grid(row=1, column=0, sticky="nsew")

    self.deferred_observability_tab = ttk.Frame(
        self.operator_notebook,
        style="Card.TFrame",
        padding=8,
    )
    self.deferred_observability_tab.grid_rowconfigure(1, weight=1)
    self.deferred_observability_tab.grid_columnconfigure(0, weight=1)
    self.operator_notebook.add(
        self.deferred_observability_tab,
        text="대기 현황",
    )
    self.deferred_observability_heading_label = ttk.Label(
        self.deferred_observability_tab,
        text="저장된 작업 확인",
        style="Header.TLabel",
    )
    self.deferred_observability_heading_label.grid(
        row=0,
        column=0,
        sticky="w",
        pady=(0, 6),
    )
    self.deferred_observability_tree = ttk.Treeview(
        self.deferred_observability_tab,
        columns=("Status", "Count", "Oldest"),
        show="headings",
        selectmode="browse",
        height=6,
        style="Deferred.Treeview",
    )
    self.deferred_observability_tree.heading(
        "Status", text="상태 구분", anchor="w"
    )
    self.deferred_observability_tree.heading(
        "Count", text="건수", anchor="center"
    )
    self.deferred_observability_tree.heading(
        "Oldest", text="최장 대기", anchor="center"
    )
    self.deferred_observability_tree.column(
        "Status", width=190, minwidth=150, stretch=True, anchor="w"
    )
    self.deferred_observability_tree.column(
        "Count", width=55, minwidth=48, stretch=False, anchor="center"
    )
    self.deferred_observability_tree.column(
        "Oldest", width=105, minwidth=85, stretch=False, anchor="center"
    )
    self.deferred_observability_tree.grid(
        row=1,
        column=0,
        sticky="nsew",
    )
    for group_key, operator_status, _states in DEFERRED_OPERATOR_STATUS_GROUPS:
        self.deferred_observability_tree.insert(
            "",
            "end",
            iid=f"deferred-{group_key}",
            values=(operator_status.replace("dependency대기", "선행조건 대기"), 0, "-"),
        )
    self.deferred_observability_detail_frame = ttk.Frame(
        self.deferred_observability_tab,
        style="Borderless.TFrame",
        height=self._operator_tree_font_linespace((self.default_font_name, 11), 11) * 5 + 4,
    )
    self.deferred_observability_detail_frame.grid_propagate(False)
    self.deferred_observability_detail_frame.grid_rowconfigure(0, weight=1)
    self.deferred_observability_detail_frame.grid_columnconfigure(0, weight=1)
    self.deferred_observability_detail_frame.grid(
        row=2,
        column=0,
        sticky="nsew",
        pady=(8, 0),
    )
    self.deferred_observability_detail_text = tk.Text(
        self.deferred_observability_detail_frame,
        height=5,
        wrap="word",
        state="normal",
        relief="flat",
        borderwidth=0,
        highlightthickness=0,
        padx=2,
        pady=2,
        background=self.colors["card_background"],
        foreground=self.colors["text_subtle"],
        font=(self.default_font_name, 11),
        takefocus=True,
    )
    self.deferred_observability_detail_scrollbar = ttk.Scrollbar(
        self.deferred_observability_detail_frame,
        orient=tk.VERTICAL,
        command=self.deferred_observability_detail_text.yview,
    )
    self.deferred_observability_detail_text.configure(
        yscrollcommand=self.deferred_observability_detail_scrollbar.set
    )
    self.deferred_observability_detail_text.grid(
        row=0,
        column=0,
        sticky="nsew",
    )
    self.deferred_observability_detail_scrollbar.grid(
        row=0,
        column=1,
        sticky="ns",
    )
    self.deferred_observability_detail_text.insert(
        "1.0",
        "로컬 대기 상태를 읽는 중입니다.",
    )
    self.deferred_observability_detail_text.configure(state="disabled")
    # Compatibility alias for renderer-focused tests and integrations that
    # previously treated this field as a Label-shaped display widget.
    self.deferred_observability_detail_label = (
        self.deferred_observability_detail_text
    )
    self.deferred_observability_alert_label = ttk.Label(
        self.deferred_observability_tab,
        text="경보 없음",
        style="Status.TLabel",
        justify=tk.LEFT,
        anchor="w",
        wraplength=max(260, panes.right_width - 42),
    )
    self.deferred_observability_alert_label.grid(
        row=3,
        column=0,
        sticky="ew",
        pady=(6, 0),
    )
    deferred_thresholds = dict(DEFERRED_ALERT_THRESHOLDS)
    self.deferred_observability_threshold_label = ttk.Label(
        self.deferred_observability_tab,
        text=(
            "경보 기준 · 재시도 "
            f"{deferred_thresholds['oldest_retry_wait_seconds'] // 60}분 · "
            "선행조건 "
            f"{deferred_thresholds['waiting_dependency_seconds'] // 3600}시간 · "
            "관리자 +"
            f"{deferred_thresholds['operator_review_increase']} · 봉인 검증 "
            f"{deferred_thresholds['repeated_seal_failure_count']}회 · 순서 지연 "
            f"{deferred_thresholds['partition_starvation_seconds'] // 60}분"
        ),
        style="Status.TLabel",
        justify=tk.LEFT,
        anchor="w",
        wraplength=max(260, panes.right_width - 42),
    )
    self.deferred_observability_threshold_label.grid(
        row=4,
        column=0,
        sticky="ew",
        pady=(6, 0),
    )
