"""Central scan pane construction with existing owner callbacks."""


def create_scan_status(
    self,
    *,
    profile,
    panes,
    tk,
    ttk,
    LABEL_MATCH_CENTRAL_INHERIT_ALL_SCAN_COUNT,
):
    self.operator_center_pane = ttk.Frame(
        self.operator_workbench_frame,
        style="Card.TFrame",
        padding=profile["card_padding"],
    )
    self.operator_center_pane.grid(
        row=0,
        column=1,
        sticky="nsew",
        padx=(panes.gap, panes.gap),
    )
    self.top_card = self.operator_center_pane
    self.operator_center_pane.grid_columnconfigure(0, weight=1)
    self.operator_center_pane.grid_rowconfigure(5, weight=1)

    self.big_display_label = ttk.Label(
        self.operator_center_pane,
        text=self._idle_instruction_text(),
        anchor="center",
        justify=tk.CENTER,
        wraplength=max(420, panes.center_width - 40),
        font=(self.default_font_name, 34, "bold"),
    )
    self.big_display_label.grid(row=0, column=0, sticky="ew", pady=(0, 8))

    self.progress_frame = ttk.Frame(self.operator_center_pane, style="Borderless.TFrame")
    self.progress_frame.grid(row=1, column=0, sticky="ew", pady=(0, 8))
    self.progress_frame.grid_columnconfigure(0, weight=1)
    self.step_rail_frame = ttk.Frame(self.progress_frame, style="Borderless.TFrame")
    self.step_rail_frame.grid(row=0, column=0, sticky="ew")
    self.step_labels = []
    initial_standard_phs2 = self._standard_phs2_workflow_expected()
    for index, step_name in enumerate(self.STEP_NAMES):
        self.step_rail_frame.grid_columnconfigure(index, weight=1, uniform="scan_steps")
        step_label = tk.Label(
            self.step_rail_frame,
            text=f"{index + 1}. {step_name}",
            font=(self.default_font_name, 11, "bold"),
            padx=6,
            pady=5,
            bd=1,
            relief="solid",
            anchor="center",
        )
        step_label.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
        if initial_standard_phs2:
            if index == 0:
                step_label.configure(text="1. PHS2 현품표")
            else:
                step_label.grid_remove()
        self.step_labels.append(step_label)
    self.progress_bar = ttk.Progressbar(
        self.progress_frame,
        orient="horizontal",
        mode="determinate",
        maximum=(
            LABEL_MATCH_CENTRAL_INHERIT_ALL_SCAN_COUNT
            if initial_standard_phs2
            else self.TOTAL_SCAN_COUNT
        ),
        style="green.Horizontal.TProgressbar",
    )
    self.progress_bar.grid(row=1, column=0, sticky="ew", pady=(6, 0))

    # Exactly one notice location; neutral guidance uses the same region.
    self.workflow_notice_frame = tk.Frame(
        self.operator_center_pane,
        bg="#EFF6FF",
        highlightbackground=self.colors["primary"],
        highlightthickness=1,
        bd=0,
    )
    self.workflow_notice_frame.grid(row=2, column=0, sticky="ew", pady=(0, 8))
    self.workflow_notice_frame.grid_columnconfigure(0, weight=1)
    self.workflow_notice_title_label = tk.Label(
        self.workflow_notice_frame,
        text="스캐너 준비",
        bg="#EFF6FF",
        fg=self.colors["primary"],
        font=(self.default_font_name, 12, "bold"),
        anchor="w",
    )
    self.workflow_notice_title_label.grid(row=0, column=0, sticky="ew", padx=12, pady=(7, 0))
    self.workflow_notice_label = tk.Label(
        self.workflow_notice_frame,
        text="현품표를 스캔하세요.",
        bg="#EFF6FF",
        fg=self.colors["text"],
        font=(self.default_font_name, 11),
        anchor="w",
        justify=tk.LEFT,
        wraplength=max(360, panes.center_width - 170),
    )
    self.workflow_notice_label.grid(row=1, column=0, sticky="ew", padx=12, pady=(1, 7))
    self.workflow_notice_action_button = ttk.Button(
        self.workflow_notice_frame,
        text="확인",
        command=self._acknowledge_workflow_notice,
        style="Action.TButton",
    )
    self.workflow_notice_action_button.grid(row=0, column=1, rowspan=2, sticky="e", padx=10, pady=7)
    self.workflow_notice_action_button.grid_remove()


def create_scan_details(
    self,
    *,
    panes,
    tk,
    ttk,
):
    self.view_mode_label = ttk.Label(
        self.operator_center_pane,
        text="",
        style="ViewMode.TLabel",
        anchor="center",
    )
    self.view_mode_label.grid(row=2, column=0, sticky="ew")
    self.view_mode_label.grid_remove()

    input_frame = ttk.Frame(self.operator_center_pane, style="Borderless.TFrame")
    self.operator_input_frame = input_frame
    input_frame.grid(row=3, column=0, sticky="ew", pady=(0, 8))
    input_frame.grid_columnconfigure(1, weight=1)
    self.operator_scan_input_label = ttk.Label(
        input_frame,
        text="스캔 입력",
        style="Header.TLabel",
    )
    self.operator_scan_input_label.grid(
        row=0, column=0, padx=(0, 12), sticky="w"
    )
    self.entry = ttk.Entry(
        input_frame,
        style="TEntry",
        state="disabled",
        font=(self.default_font_name, 18),
    )
    self.entry.grid(row=0, column=1, sticky="ew", ipady=8)
    self.entry.bind("<Return>", self._handle_scan_enter)

    self.operator_details_button = ttk.Button(
        self.operator_center_pane,
        text="작업 상세 보기 ▾",
        command=self._toggle_operator_details,
        style="Control.TButton",
    )
    self.operator_details_button.grid(row=4, column=0, sticky="w", pady=(0, 8))
    self.live_scan_notebook = ttk.Notebook(self.operator_center_pane)
    self.live_scan_notebook.grid(row=5, column=0, sticky="nsew")
    self.qa_scan_frame = ttk.Frame(self.live_scan_notebook, style="Card.TFrame")
    self.qa_scan_frame.grid_rowconfigure(0, weight=1)
    self.qa_scan_frame.grid_columnconfigure(0, weight=1)
    self.live_scan_notebook.add(self.qa_scan_frame, text="현재 세트 0/5")
    self.qa_scan_tree = ttk.Treeview(
        self.qa_scan_frame,
        columns=("Stage", "Value", "State"),
        show="headings",
        selectmode="browse",
        height=5,
        takefocus=True,
    )
    self.qa_scan_tree.heading("Stage", text="단계", anchor="center")
    self.qa_scan_tree.heading("Value", text="실제 스캔 값", anchor="w")
    self.qa_scan_tree.heading("State", text="상태", anchor="center")
    self.qa_scan_tree.column("Stage", width=115, minwidth=90, stretch=False, anchor="center")
    self.qa_scan_tree.column("Value", width=max(280, panes.center_width - 260), minwidth=180, stretch=True, anchor="w")
    self.qa_scan_tree.column("State", width=90, minwidth=72, stretch=False, anchor="center")
    self.qa_scan_tree.grid(row=0, column=0, sticky="nsew")
    self.qa_scan_tree.bind(
        "<<TreeviewSelect>>",
        self._on_qa_scan_selection_changed,
    )
    self.current_set_tree = self.qa_scan_tree

    self.qa_scan_detail_frame = ttk.Frame(
        self.qa_scan_frame,
        style="Borderless.TFrame",
        padding=(6, 5, 6, 4),
    )
    self.qa_scan_detail_frame.grid(
        row=1,
        column=0,
        sticky="nsew",
        pady=(4, 0),
    )
    self.qa_scan_detail_frame.grid_columnconfigure(1, weight=1)
    self.qa_scan_detail_frame.grid_rowconfigure(1, weight=1)
    self.qa_scan_detail_title_label = ttk.Label(
        self.qa_scan_detail_frame,
        text="선택 행 원문",
        style="Header.TLabel",
    )
    self.qa_scan_detail_title_label.grid(row=0, column=0, sticky="w")
    self.qa_scan_detail_metadata_label = ttk.Label(
        self.qa_scan_detail_frame,
        text="단계: -  |  상태: -",
        style="Status.TLabel",
        anchor="w",
    )
    self.qa_scan_detail_metadata_label.grid(
        row=0,
        column=1,
        columnspan=2,
        sticky="ew",
        padx=(12, 0),
    )
    self.qa_scan_detail_text = tk.Text(
        self.qa_scan_detail_frame,
        height=2,
        wrap="char",
        font=("Consolas", 10),
        bg=self.colors["card_background"],
        fg=self.colors["text"],
        relief="solid",
        bd=1,
        padx=6,
        pady=3,
        takefocus=0,
    )
    self.qa_scan_detail_scrollbar = ttk.Scrollbar(
        self.qa_scan_detail_frame,
        orient=tk.VERTICAL,
        command=self.qa_scan_detail_text.yview,
    )
    self.qa_scan_detail_text.configure(
        yscrollcommand=self.qa_scan_detail_scrollbar.set,
    )
    self.qa_scan_detail_text.grid(
        row=1,
        column=0,
        columnspan=2,
        sticky="nsew",
        pady=(3, 0),
    )
    self.qa_scan_detail_scrollbar.grid(
        row=1,
        column=2,
        sticky="ns",
        pady=(3, 0),
    )
    self.qa_scan_detail_text.insert(
        "1.0",
        "현재 세트 행을 선택하면 수락된 스캔 원문을 확인할 수 있습니다.",
    )
    self.qa_scan_detail_text.configure(state="disabled")

    self.operator_task_detail_frame = ttk.Frame(self.qa_scan_frame, padding=(6, 4))
    self.operator_task_detail_frame.grid(row=2, column=0, sticky="ew")
    self.operator_task_detail_frame.grid_columnconfigure(0, weight=1)
    self.operator_task_detail_text = tk.Text(
        self.operator_task_detail_frame, height=4, wrap="word",
        font=(self.default_font_name, 10), relief="flat",
        bg=self.colors["card_background"], fg=self.colors["text"],
    )
    task_detail_scrollbar = ttk.Scrollbar(
        self.operator_task_detail_frame, orient=tk.VERTICAL,
        command=self.operator_task_detail_text.yview,
    )
    self.operator_task_detail_text.configure(yscrollcommand=task_detail_scrollbar.set, state="disabled")
    self.operator_task_detail_text.grid(row=0, column=0, sticky="nsew")
    task_detail_scrollbar.grid(row=0, column=1, sticky="ns")

    self.exact_rescan_frame = ttk.Frame(self.live_scan_notebook, style="Card.TFrame")
    self.exact_rescan_frame.grid_rowconfigure(0, weight=1)
    self.exact_rescan_frame.grid_columnconfigure(0, weight=1)
    self.live_scan_notebook.add(self.exact_rescan_frame, text="F4 전체 재스캔")
    self.exact_rescan_tree = ttk.Treeview(
        self.exact_rescan_frame,
        columns=("Order", "Value"),
        show="headings",
        selectmode="browse",
    )
    self.exact_rescan_tree.heading("Order", text="순서", anchor="center")
    self.exact_rescan_tree.heading("Value", text="실제 F4 재스캔 값", anchor="w")
    self.exact_rescan_tree.column("Order", width=80, minwidth=60, stretch=False, anchor="center")
    self.exact_rescan_tree.column("Value", width=max(320, panes.center_width - 150), minwidth=220, stretch=True, anchor="w")
    self.exact_rescan_tree.grid(row=0, column=0, sticky="nsew")
    self.exact_rescan_tree.bind(
        "<<TreeviewSelect>>",
        self._on_exact_rescan_selection_changed,
    )
    self.exact_rescan_detail_frame = ttk.Frame(
        self.exact_rescan_frame,
        style="Borderless.TFrame",
        padding=(6, 5, 6, 4),
    )
    self.exact_rescan_detail_frame.grid(
        row=1,
        column=0,
        sticky="nsew",
        pady=(4, 0),
    )
    self.exact_rescan_detail_frame.grid_columnconfigure(1, weight=1)
    self.exact_rescan_detail_frame.grid_rowconfigure(1, weight=1)
    self.exact_rescan_detail_title_label = ttk.Label(
        self.exact_rescan_detail_frame,
        text="선택 F4 원문",
        style="Header.TLabel",
    )
    self.exact_rescan_detail_title_label.grid(row=0, column=0, sticky="w")
    self.exact_rescan_detail_metadata_label = ttk.Label(
        self.exact_rescan_detail_frame,
        text="순서: -",
        style="Status.TLabel",
        anchor="w",
    )
    self.exact_rescan_detail_metadata_label.grid(
        row=0,
        column=1,
        columnspan=2,
        sticky="ew",
        padx=(12, 0),
    )
    self.exact_rescan_detail_text = tk.Text(
        self.exact_rescan_detail_frame,
        height=2,
        wrap="char",
        font=("Consolas", 10),
        bg=self.colors["card_background"],
        fg=self.colors["text"],
        relief="solid",
        bd=1,
        padx=6,
        pady=3,
        takefocus=0,
    )
    self.exact_rescan_detail_scrollbar = ttk.Scrollbar(
        self.exact_rescan_detail_frame,
        orient=tk.VERTICAL,
        command=self.exact_rescan_detail_text.yview,
    )
    self.exact_rescan_detail_text.configure(
        yscrollcommand=self.exact_rescan_detail_scrollbar.set,
    )
    self.exact_rescan_detail_text.grid(
        row=1,
        column=0,
        columnspan=2,
        sticky="nsew",
        pady=(3, 0),
    )
    self.exact_rescan_detail_scrollbar.grid(
        row=1,
        column=2,
        sticky="ns",
        pady=(3, 0),
    )
    self.exact_rescan_detail_text.insert(
        "1.0",
        "F4 재스캔 행을 선택하면 전체 원문을 확인할 수 있습니다.",
    )
    self.exact_rescan_detail_text.configure(state="disabled")
    hide_exact_tab = getattr(self.live_scan_notebook, "hide", None)
    if callable(hide_exact_tab):
        hide_exact_tab(self.exact_rescan_frame)
    self.operator_last_scan_label = ttk.Label(
        self.operator_center_pane,
        text="마지막 정상 스캔: -",
        style="Status.TLabel",
        anchor="w",
        wraplength=max(380, panes.center_width - 30),
    )
    self.operator_last_scan_label.grid(row=6, column=0, sticky="ew", pady=(8, 0))
    self.status_label = self.operator_last_scan_label
