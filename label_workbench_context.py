"""Workbench context and common frame/footer construction."""


def create_context_pane(
    self,
    *,
    tk,
    ttk,
    TclError,
    build_operator_layout,
    build_style_tokens,
    _label_match_operator_context,
):
    profile = getattr(self, "ui_profile", self.UI_PROFILES["standard"])
    outer_padding = int(profile["outer_padding"])
    try:
        # ``winfo_screenwidth`` can be the combined virtual desktop on a
        # multi-monitor station.  Capping the bootstrap size prevents that
        # value from becoming a permanent multi-thousand-pixel Treeview
        # requisition before the first real <Configure> event.
        current_width = int(self.winfo_width())
        bootstrap_width = current_width if current_width > 100 else min(
            1920, int(self.winfo_screenwidth())
        )
        initial_width = max(980, bootstrap_width - outer_padding * 2)
        initial_height = max(640, int(self.winfo_height()) - outer_padding * 2 - 70)
    except (TclError, AttributeError, RecursionError, TypeError, ValueError):
        initial_width, initial_height = 1440, 830
    operator_scale = float(self.__dict__.get("scale_factor", 1.0) or 1.0)
    self.operator_layout_metrics = build_operator_layout(
        initial_width,
        initial_height,
        operator_scale,
    )
    self.operator_style_tokens = build_style_tokens(
        self.operator_layout_metrics.profile.name,
        operator_scale,
    )

    self.main_frame = ttk.Frame(self, padding=outer_padding)
    main_frame = self.main_frame
    main_frame.pack(fill=tk.BOTH, expand=True)
    main_frame.grid_rowconfigure(1, weight=1)
    main_frame.grid_columnconfigure(0, weight=1)

    # Header: context and low-frequency controls stay out of the scan path.
    self.operator_header_frame = ttk.Frame(main_frame, style="Card.TFrame", padding=(14, 8))
    self.operator_header_frame.grid(row=0, column=0, sticky="ew", pady=(0, profile["section_gap"]))
    self.operator_header_frame.grid_columnconfigure(1, weight=1)
    self.operator_title_label = ttk.Label(
        self.operator_header_frame,
        text="Label Match · 포장 라벨 검증",
        style="Header.TLabel",
    )
    self.operator_title_label.grid(row=0, column=0, sticky="w")
    self.operator_header_context_label = ttk.Label(
        self.operator_header_frame,
        text=_label_match_operator_context(
            self.__dict__.get('worker_name', '-')
        ),
        style="Status.TLabel",
    )
    self.operator_header_context_label.grid(row=0, column=1, sticky="e", padx=(12, 16))
    self.top_right_frame = ttk.Frame(self.operator_header_frame, style="Borderless.TFrame")
    self.top_right_frame.grid(row=0, column=2, sticky="e")
    self.clock_label = ttk.Label(self.top_right_frame, text="", style="Status.TLabel")
    self.clock_label.pack(side=tk.LEFT, padx=(0, 12))
    self.settings_button = ttk.Button(
        self.top_right_frame,
        text="설정",
        command=self.open_settings_window,
        style="Control.TButton",
    )
    self.settings_button.pack(side=tk.LEFT, padx=(0, 6))
    self.about_button = ttk.Button(
        self.top_right_frame,
        text="정보",
        command=self._show_about_window,
        style="Control.TButton",
    )
    self.about_button.pack(side=tk.LEFT)

    # Persistent three-column desk.
    self.operator_workbench_frame = ttk.Frame(main_frame)
    self.operator_workbench_frame.grid(row=1, column=0, sticky="nsew")
    self.operator_workbench_frame.grid_rowconfigure(0, weight=1)
    self.workbench_frame = self.operator_workbench_frame

    panes = self.operator_layout_metrics.panes
    self.operator_workbench_frame.grid_columnconfigure(0, minsize=panes.left_width)
    self.operator_workbench_frame.grid_columnconfigure(1, weight=1, minsize=panes.center_width)
    self.operator_workbench_frame.grid_columnconfigure(2, minsize=panes.right_width)

    self.operator_left_pane = ttk.Frame(
        self.operator_workbench_frame,
        style="Card.TFrame",
        padding=profile["card_padding"],
    )
    self.operator_left_pane.grid(row=0, column=0, sticky="nsew")
    self.left_context_card = self.operator_left_pane
    self.operator_left_pane.grid_columnconfigure(0, weight=1)
    self.operator_left_heading_label = ttk.Label(
        self.operator_left_pane,
        text="현재 작업",
        style="Header.TLabel",
    )
    self.operator_left_heading_label.grid(
        row=0, column=0, sticky="w", pady=(0, 12)
    )
    self.operator_item_stage_label = ttk.Label(
        self.operator_left_pane,
        text="현품표 대기",
        style="Success.TLabel",
        wraplength=max(150, panes.left_width - 40),
    )
    self.operator_item_stage_label.grid(row=1, column=0, sticky="ew", pady=(0, 12))

    self.operator_item_card = ttk.Frame(self.operator_left_pane, style="Borderless.TFrame")
    self.operator_item_card.grid(row=2, column=0, sticky="ew")
    self.operator_item_card.grid_columnconfigure(0, weight=1)
    self.operator_item_code_label = ttk.Label(
        self.operator_item_card,
        text="현품표 -",
        style="Header.TLabel",
        wraplength=max(150, panes.left_width - 44),
    )
    self.operator_item_code_label.grid(row=0, column=0, sticky="ew")
    self.operator_item_name_label = ttk.Label(
        self.operator_item_card,
        text="품목 -",
        style="Status.TLabel",
        wraplength=max(150, panes.left_width - 44),
    )
    self.operator_item_name_label.grid(row=1, column=0, sticky="ew", pady=(8, 0))
    self.operator_item_spec_label = ttk.Label(
        self.operator_item_card,
        text="규격 -",
        style="Status.TLabel",
        wraplength=max(150, panes.left_width - 44),
    )
    self.operator_item_spec_label.grid(row=2, column=0, sticky="ew", pady=(5, 0))
    self.operator_item_phase_label = ttk.Label(
        self.operator_item_card,
        text="차수 -",
        style="Status.TLabel",
    )
    self.operator_item_phase_label.grid(row=3, column=0, sticky="ew", pady=(5, 0))
    self.operator_left_divider = ttk.Frame(
        self.operator_left_pane,
        style="TFrame",
        height=1,
    )
    self.operator_left_divider.grid(row=3, column=0, sticky="ew", pady=16)
    self.operator_membership_heading_label = ttk.Label(
        self.operator_left_pane,
        text="포장 수량",
        style="Status.TLabel",
    )
    self.operator_membership_heading_label.grid(row=4, column=0, sticky="w")
    self.operator_membership_label = ttk.Label(
        self.operator_left_pane,
        text=(
            "PHS2 1장 · 중앙 멤버십 상속"
            if self._standard_phs2_workflow_expected()
            else "레거시 QA 5단계"
        ),
        style="Header.TLabel",
        wraplength=max(150, panes.left_width - 40),
    )
    self.operator_membership_label.grid(row=5, column=0, sticky="ew", pady=(6, 0))
    self.operator_badges_label = ttk.Label(
        self.operator_left_pane,
        text="",
        style="ViewMode.TLabel",
        wraplength=max(150, panes.left_width - 40),
    )
    self.operator_badges_label.grid(row=6, column=0, sticky="ew", pady=(12, 0))
    self.operator_badges_label.grid_remove()
    return profile, main_frame, panes


def create_status_footer(
    self,
    *,
    main_frame,
    profile,
    tk,
    ttk,
):
    self.operator_status_frame = ttk.Frame(main_frame)
    self.operator_status_frame.grid(row=2, column=0, sticky="ew", pady=(profile["bottom_gap"], 0))
    self.operator_status_frame.grid_columnconfigure(0, weight=1)
    self.save_status_label = ttk.Label(
        self.operator_status_frame,
        text="",
        style="Save.Success.TLabel",
        background=self.colors["background"],
    )
    self.save_status_label.grid(row=0, column=0, sticky="w")
    self.operator_footer_label = ttk.Label(
        self.operator_status_frame,
        text="",
        style="Status.TLabel",
    )
    self.operator_footer_label.grid(row=0, column=1, sticky="e")

    self.loading_overlay = ttk.Frame(main_frame, style="Overlay.TFrame")
    loading_content_frame = ttk.Frame(self.loading_overlay, style="Overlay.TFrame")
    loading_content_frame.pack(expand=True)
    ttk.Label(
        loading_content_frame,
        text="데이터를 불러오는 중입니다...",
        style="Loading.TLabel",
    ).pack(pady=(0, 15))
    self.loading_progressbar = ttk.Progressbar(loading_content_frame, mode="indeterminate", length=400)
    self.loading_progressbar.pack(pady=15)
