"""History, summary and operator actions pane construction."""


def create_history_pane(
    self,
    *,
    tk,
    ttk,
    create_completed_tray_cancel_button,
):
    self.history_card = ttk.Frame(self.operator_notebook, style="Card.TFrame", padding=8)
    history_card = self.history_card
    self.history_tab = history_card
    self.operator_history_tab = history_card
    self.operator_notebook.add(history_card, text="스캔 기록")
    history_card.grid_rowconfigure(1, weight=1)
    history_card.grid_columnconfigure(0, weight=1)
    self.hist_header_frame = ttk.Frame(history_card, style="Borderless.TFrame")
    self.hist_header_frame.grid(row=0, column=0, sticky="ew", pady=(0, 6))
    self.hist_header_frame.grid_columnconfigure(1, weight=1)
    self._history_header_full_text = "스캔 기록"
    self.hist_header_label = ttk.Label(
        self.hist_header_frame,
        text=self._history_header_full_text,
        style="Header.TLabel",
    )
    self.hist_header_label.grid(row=0, column=0, sticky="w")
    self.hist_control_frame = ttk.Frame(self.hist_header_frame, style="Borderless.TFrame")
    self.hist_control_frame.grid(row=0, column=2, sticky="e")
    self.today_button = ttk.Button(
        self.hist_control_frame,
        text="오늘",
        style="Control.TButton",
        command=self._reload_today_history,
    )
    self.today_button.pack(side=tk.LEFT, padx=(0, 4))
    self.date_search_button = ttk.Button(
        self.hist_control_frame,
        text="조회",
        style="Control.TButton",
        command=self._prompt_for_date_and_reload,
    )
    self.date_search_button.pack(side=tk.LEFT, padx=(0, 4))
    self.decrease_font_button = ttk.Button(
        self.hist_control_frame,
        text="-",
        style="Control.TButton",
        command=self._decrease_tree_font,
    )
    self.decrease_font_button.pack(side=tk.LEFT)
    self.increase_font_button = ttk.Button(
        self.hist_control_frame,
        text="+",
        style="Control.TButton",
        command=self._increase_tree_font,
    )
    self.increase_font_button.pack(side=tk.LEFT)
    tree_frame_hist = ttk.Frame(history_card, style="Card.TFrame")
    tree_frame_hist.grid(row=1, column=0, sticky="nsew")
    tree_frame_hist.grid_rowconfigure(0, weight=1)
    tree_frame_hist.grid_columnconfigure(0, weight=1)
    hist_cols = list(self.hist_proportions.keys())
    v_scroll_hist = ttk.Scrollbar(tree_frame_hist, orient=tk.VERTICAL)
    h_scroll_hist = ttk.Scrollbar(tree_frame_hist, orient=tk.HORIZONTAL)
    self.history_tree = ttk.Treeview(
        tree_frame_hist,
        columns=hist_cols,
        displaycolumns=("Input1", "Result", "Timestamp"),
        show="headings",
        yscrollcommand=v_scroll_hist.set,
        xscrollcommand=h_scroll_hist.set,
        selectmode="extended",
    )
    v_scroll_hist.config(command=self.history_tree.yview)
    h_scroll_hist.config(command=self.history_tree.xview)
    for col, labels in self.HISTORY_HEADING_LABELS.items():
        self.history_tree.heading(
            col,
            text=labels[0],
            anchor="center",
            command=lambda c=col: self._treeview_sort_column(self.history_tree, c, False),
        )
        self.history_tree.column(col, anchor="center", minwidth=60, stretch=False)
    v_scroll_hist.grid(row=0, column=1, sticky="ns")
    h_scroll_hist.grid(row=1, column=0, sticky="ew")
    self.history_tree.grid(row=0, column=0, sticky="nsew")
    self.history_tree.bind("<Configure>", self._resize_all_columns)
    self.history_tree.bind("<ButtonRelease-1>", self._on_history_tree_resize_release)
    self.history_tree.bind("<<TreeviewSelect>>", self._on_history_selection_changed)
    self.history_tree.bind("<Double-1>", self._show_selected_history_detail_window)

    self.history_detail_frame = ttk.Frame(history_card, style="Borderless.TFrame")
    self.history_detail_frame.grid(row=2, column=0, sticky="ew", pady=(6, 0))
    self.history_detail_frame.grid_columnconfigure(0, weight=1)
    detail_header_frame = ttk.Frame(self.history_detail_frame, style="Borderless.TFrame")
    detail_header_frame.grid(row=0, column=0, sticky="ew")
    detail_header_frame.grid_columnconfigure(0, weight=1)
    self.history_detail_modal_button = ttk.Button(
        detail_header_frame,
        text="원문",
        style="Control.TButton",
        command=self._show_selected_history_detail_window,
        state="disabled",
    )
    self.history_detail_modal_button.grid(row=0, column=1, sticky="e")
    self.history_detail_copy_button = ttk.Button(
        detail_header_frame,
        text="복사",
        style="Control.TButton",
        command=self._copy_selected_history_barcodes,
        state="disabled",
    )
    self.history_detail_copy_button.grid(row=0, column=2, sticky="e", padx=(4, 0))
    self.history_detail_text = tk.Text(
        self.history_detail_frame,
        height=3,
        wrap="word",
        font=("Consolas", 9),
        bg=self.colors["card_background"],
        fg=self.colors["text"],
        relief="solid",
        bd=1,
        padx=6,
        pady=4,
    )
    self.history_detail_text.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(4, 0))
    self.history_detail_text.insert("1.0", "기록을 선택하면 스캔 원문을 확인할 수 있습니다.")
    self.history_detail_text.configure(state="disabled")

    self.history_context_menu = tk.Menu(self, tearoff=0, font=(self.default_font_name, 14))
    self.history_context_menu.add_command(label="바코드 원문 보기", command=self._show_selected_history_detail_window)
    self.history_context_menu.add_command(label="바코드 원문 복사", command=self._copy_selected_history_barcodes)
    self.history_context_menu.add_separator()
    self.history_context_menu.add_command(label=self.HISTORY_DELETE_ACTION_TEXT, command=self._delete_selected_row)
    self.history_tree.bind("<Button-3>", self._show_history_context_menu)

    self.summary_card = ttk.Frame(self.operator_notebook, style="Card.TFrame", padding=8)
    summary_card = self.summary_card
    self.summary_tab = summary_card
    self.operator_summary_tab = summary_card
    self.operator_notebook.add(summary_card, text="통과 요약")
    summary_card.grid_rowconfigure(1, weight=1)
    summary_card.grid_columnconfigure(0, weight=1)
    self.summary_header_frame = ttk.Frame(summary_card, style="Borderless.TFrame")
    self.summary_header_frame.grid(row=0, column=0, sticky="ew", pady=(0, 6))
    self.summary_header_frame.grid_columnconfigure(0, weight=1)
    self.summary_header_label = ttk.Label(
        self.summary_header_frame,
        text="누적 통과 코드",
        style="Header.TLabel",
    )
    self.summary_header_label.grid(row=0, column=0, sticky="w")
    self.summary_date_label = ttk.Label(
        self.summary_header_frame,
        text="날짜 -",
        style="SummaryDate.TLabel",
        anchor="center",
    )
    self.summary_date_label.grid(row=0, column=1, sticky="e")
    tree_frame_sum = ttk.Frame(summary_card, style="Card.TFrame")
    tree_frame_sum.grid(row=1, column=0, sticky="nsew")
    tree_frame_sum.grid_rowconfigure(0, weight=1)
    tree_frame_sum.grid_columnconfigure(0, weight=1)
    summary_cols = list(self.summary_proportions.keys())
    v_scroll_sum = ttk.Scrollbar(tree_frame_sum, orient=tk.VERTICAL)
    self.summary_tree = ttk.Treeview(
        tree_frame_sum,
        columns=summary_cols,
        show="headings",
        yscrollcommand=v_scroll_sum.set,
    )
    v_scroll_sum.config(command=self.summary_tree.yview)
    for column in summary_cols:
        self.summary_tree.heading(
            column,
            text=self.SUMMARY_HEADING_LABELS[column][0],
            anchor="center",
            command=lambda c=column: self._treeview_sort_column(self.summary_tree, c, False),
        )
    self.summary_tree.column("Code", anchor="w", minwidth=150, stretch=True)
    self.summary_tree.column("Phase", anchor="center", minwidth=55, stretch=False)
    self.summary_tree.column("Count", anchor="center", minwidth=60, stretch=False)
    self.summary_tree.grid(row=0, column=0, sticky="nsew")
    v_scroll_sum.grid(row=0, column=1, sticky="ns")
    self.summary_tree.bind("<Configure>", self._resize_all_columns)
    self.summary_tree.bind("<ButtonRelease-1>", self._on_summary_tree_resize_release)

    self.operator_action_frame = ttk.Frame(self.operator_right_pane)
    self.operator_action_frame.grid(row=1, column=0, sticky="ew", pady=(10, 0))
    self.operator_action_frame.grid_columnconfigure(
        (0, 1, 2),
        weight=1,
        uniform="operator_actions",
    )
    self.bottom_frame = self.operator_action_frame
    self.manual_complete_button = ttk.Button(
        self.operator_action_frame,
        text=(
            "포장 완료 (F3)"
            if self._standard_phs2_workflow_expected()
            else self.MANUAL_COMPLETE_BUTTON_TEXT
        ),
        command=self._prompt_manual_complete,
        style=self.MANUAL_COMPLETE_BUTTON_STYLE,
        state="disabled",
    )
    self.manual_complete_button.grid(row=0, column=0, sticky="nsew", padx=(0, 4), pady=(0, 4))
    self.exact_rescan_button = ttk.Button(
        self.operator_action_frame,
        text=(
            "제품 교체 (F4)"
            if self._standard_phs2_workflow_expected()
            else self.EXACT_RESCAN_BUTTON_TEXT
        ),
        command=self._handle_f4_action,
        style=self.MANUAL_COMPLETE_BUTTON_STYLE,
        state="disabled",
    )
    self.exact_rescan_button.grid(
        row=0,
        column=1,
        sticky="nsew",
        padx=4,
        pady=(0, 4),
    )
    self.reset_button = ttk.Button(
        self.operator_action_frame,
        text=self.CURRENT_SET_CANCEL_BUTTON_TEXT,
        command=lambda: self._reset_current_set(full_reset=True),
        style=self.CURRENT_SET_CANCEL_BUTTON_STYLE,
    )
    self.reset_button.grid(row=1, column=0, sticky="nsew", padx=(0, 4), pady=(4, 0))
    self.cancel_tray_button = create_completed_tray_cancel_button(self.operator_action_frame)
    self.cancel_tray_button.grid(
        row=1,
        column=1,
        columnspan=2,
        sticky="nsew",
        padx=(4, 0),
        pady=(4, 0),
    )
    self.phs_label_exchange_button = ttk.Button(
        self.operator_action_frame,
        text=self.PHS_LABEL_EXCHANGE_BUTTON_TEXT,
        command=self._handle_phs_label_exchange_shortcut,
        style=self.MANUAL_COMPLETE_BUTTON_STYLE,
        state="disabled",
    )
    self.phs_label_exchange_button.grid(
        row=0,
        column=2,
        sticky="nsew",
        padx=(4, 0),
        pady=(0, 4),
    )

    self.bind("<F1>", lambda event: self._handle_workflow_shortcut("f1", event))
    self.bind("<F2>", lambda event: self._handle_workflow_shortcut("f2", event))
    self.bind("<F3>", lambda event: self._handle_workflow_shortcut("f3", event))
    self.bind("<F4>", lambda event: self._handle_workflow_shortcut("f4", event))
    self.bind(
        "<F5>",
        self._handle_phs_label_exchange_shortcut,
    )
    self.bind("<Escape>", self._handle_workflow_escape)
    self.bind("<Delete>", self._delete_selected_row_from_shortcut)
