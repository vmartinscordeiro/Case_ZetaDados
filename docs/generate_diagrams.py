"""
Diagram Generator
=================
Generates two architecture/data-model diagrams, each saved as PNG + PDF.

Outputs
-------
    docs/diagrams/pipeline_architecture.png / .pdf
    docs/diagrams/star_schema.png / .pdf

Usage
-----
    python docs/generate_diagrams.py
"""

from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
from matplotlib.backends.backend_pdf import PdfPages

OUTPUT_DIR = Path(__file__).parent / "diagrams"
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Colour palette ────────────────────────────────────────────────────────────
C = {
    "bg":         "#F8F9FA",
    "source":     "#1F4E79",
    "source_lt":  "#BDD7EE",
    "bronze":     "#C65911",
    "bronze_lt":  "#FCE4D6",
    "silver":     "#44546A",
    "silver_lt":  "#D6DCE4",
    "gold":       "#7F6000",
    "gold_lt":    "#FFF2CC",
    "fact":       "#1F4E79",
    "fact_lt":    "#BDD7EE",
    "dim":        "#375623",
    "dim_lt":     "#C6EFCE",
    "white":      "#FFFFFF",
    "dark":       "#1A1A1A",
    "mid":        "#555555",
    "light":      "#888888",
    "arrow":      "#444444",
}


# ── Shared drawing helpers ────────────────────────────────────────────────────

def _box(ax, x, y, w, h, fill, edge, radius=0.15, lw=1.5, zorder=3):
    ax.add_patch(FancyBboxPatch(
        (x, y), w, h,
        boxstyle=f"round,pad=0,rounding_size={radius}",
        facecolor=fill, edgecolor=edge, linewidth=lw, zorder=zorder,
    ))


def _hdr(ax, x, y, w, h, fill, label, size=10):
    """Solid-colour header bar inside a box."""
    _box(ax, x, y, w, h, fill=fill, edge=fill, radius=0.1, lw=0, zorder=4)
    ax.text(x + w / 2, y + h / 2, label,
            fontsize=size, fontweight="bold", color=C["white"],
            ha="center", va="center", zorder=5)


def _txt(ax, x, y, s, size=8.5, color=None, bold=False, ha="center", va="center", zorder=5):
    color = color or C["dark"]
    ax.text(x, y, s, fontsize=size, fontweight="bold" if bold else "normal",
            color=color, ha=ha, va=va, zorder=zorder)


def _arrow(ax, x1, y1, x2, y2, color=None, lw=2, head=14, zorder=4):
    color = color or C["arrow"]
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="-|>", color=color,
                                lw=lw, mutation_scale=head),
                zorder=zorder)


def _save(fig, stem):
    """Save figure as PNG (150 dpi) and PDF (vector)."""
    png_path = OUTPUT_DIR / f"{stem}.png"
    pdf_path = OUTPUT_DIR / f"{stem}.pdf"
    fig.savefig(png_path, dpi=150, bbox_inches="tight", facecolor=C["bg"])
    with PdfPages(pdf_path) as pdf:
        pdf.savefig(fig, bbox_inches="tight", facecolor=C["bg"])
    print(f"  PNG: {png_path}")
    print(f"  PDF: {pdf_path}")
    plt.close(fig)


# ─────────────────────────────────────────────────────────────────────────────
# DIAGRAM 1 — Pipeline Architecture
# ─────────────────────────────────────────────────────────────────────────────

def build_pipeline_diagram():
    # Canvas: wide landscape, generous height
    fig, ax = plt.subplots(figsize=(22, 12))
    fig.patch.set_facecolor(C["bg"])
    ax.set_facecolor(C["bg"])
    ax.set_xlim(0, 22)
    ax.set_ylim(0, 12)
    ax.axis("off")

    # ── Title ────────────────────────────────────────────────────────────────
    _txt(ax, 11, 11.55, "Pipeline ETL — Arquitetura de Dados",
         size=17, bold=True, color=C["dark"])
    _txt(ax, 11, 11.15, "E-commerce  ·  Bronze → Silver → Gold  ·  Medallion Architecture",
         size=10, color=C["mid"])

    # ── Column definitions ────────────────────────────────────────────────────
    # (label, x_start, header_color, bg_color)
    cols = [
        ("FONTES DE DADOS",   0.35, C["source"], C["source_lt"]),
        ("BRONZE  (Raw)",     5.85, C["bronze"], C["bronze_lt"]),
        ("SILVER  (Clean)",  11.35, C["silver"], C["silver_lt"]),
        ("GOLD  (Analytics)",16.85, C["gold"],   C["gold_lt"]),
    ]
    COL_W = 5.0
    COL_H = 9.8
    COL_Y = 0.6

    for label, cx, hdr_c, bg_c in cols:
        _box(ax, cx, COL_Y, COL_W, COL_H, fill=bg_c, edge=hdr_c, radius=0.25, lw=2.2)
        _hdr(ax, cx, COL_Y + COL_H - 0.72, COL_W, 0.72, fill=hdr_c, label=label, size=11)

    # ── Sources column ────────────────────────────────────────────────────────
    src = [
        ("CSV Files",  "Arquivos CSV particionados\npor data (Archives/)"),
        ("REST API",   "Endpoints JSON\ncom suporte a paginação"),
        ("Database",   "SQL via SQLAlchemy\n(PostgreSQL · MySQL · SQLite)"),
    ]
    for i, (title, desc) in enumerate(src):
        by = COL_Y + 0.55 + i * 2.85
        _box(ax, 0.65, by, 4.4, 2.4, fill=C["white"], edge=C["source"], radius=0.15, lw=1.5)
        _txt(ax, 2.85, by + 1.65, title, size=10, bold=True, color=C["source"])
        _txt(ax, 2.85, by + 0.9, desc, size=8.5, color=C["dark"])

    _box(ax, 0.65, COL_Y + COL_H - 1.55, 4.4, 0.72,
         fill=C["source_lt"], edge=C["source"], radius=0.12, lw=1.5)
    _txt(ax, 2.85, COL_Y + COL_H - 1.17,
         "BaseConnector  (ABC interface)", size=9, bold=True, color=C["source"])

    # ── Bronze column ─────────────────────────────────────────────────────────
    _box(ax, 6.15, COL_Y + 0.45, 4.4, 4.8, fill=C["white"], edge=C["bronze"],
         radius=0.15, lw=1.5)
    _txt(ax, 8.35, COL_Y + 4.9, "BronzeIngestor", size=10, bold=True, color=C["bronze"])
    bz_lines = [
        ("• Recebe DataFrame do conector",         False),
        ("• SEM transformações de negócio",        False),
        ("• Adiciona metadados:",                  False),
        ("  _ingest_date",                         False),
        ("  _source_file",                         False),
        ("  _load_ts",                             False),
        ("• Validação Pandera tolerante",           False),
        ("  (erros logados, dados mantidos)",       False),
    ]
    for j, (line, _) in enumerate(bz_lines):
        _txt(ax, 6.35, COL_Y + 4.52 - j * 0.51, line, size=8, ha="left", color=C["dark"])

    _box(ax, 6.15, COL_Y + 5.6, 4.4, 1.6, fill=C["white"], edge=C["bronze"],
         radius=0.15, lw=1.5)
    _txt(ax, 8.35, COL_Y + 6.85, "Saída (particionada)", size=9, bold=True, color=C["bronze"])
    _txt(ax, 8.35, COL_Y + 6.45,
         "data/bronze/ingest_date=YYYY-MM-DD/", size=8, color=C["dark"])
    _txt(ax, 8.35, COL_Y + 6.1,
         "orders · order_items · shipments", size=7.5, color=C["mid"])
    _txt(ax, 8.35, COL_Y + 5.78,
         "customers · products", size=7.5, color=C["mid"])

    # ── Silver column ─────────────────────────────────────────────────────────
    _box(ax, 11.65, COL_Y + 0.45, 4.4, 5.5, fill=C["white"], edge=C["silver"],
         radius=0.15, lw=1.5)
    _txt(ax, 13.85, COL_Y + 5.6, "SilverCleaner", size=10, bold=True, color=C["silver"])
    sv_lines = [
        "• Lê Bronze do dia atual",
        "• Union com Silver existente",
        "• Drop PKs nulas (aviso)",
        "• Transformações por tabela:",
        "  - Parse timestamps (BR/ISO/mixed)",
        "  - Decimal BR: '0,00' → 0.0",
        "  - Placeholders → NaN/NaT",
        "  - Carrier/brand uppercase/title",
        "  - Valores negativos → NaN",
        "  - item_net_amount derivado",
        "• Dedup por PK (keep='last')",
        "• Validação Pandera estrita",
    ]
    for j, line in enumerate(sv_lines):
        _txt(ax, 11.85, COL_Y + 5.2 - j * 0.42, line, size=8, ha="left", color=C["dark"])

    _box(ax, 11.65, COL_Y + 6.3, 4.4, 1.1, fill=C["white"], edge=C["silver"],
         radius=0.15, lw=1.5)
    _txt(ax, 13.85, COL_Y + 7.05, "Saída — data/silver/", size=9, bold=True, color=C["silver"])
    _txt(ax, 13.85, COL_Y + 6.65,
         "5 tabelas CSV (full replace incremental)", size=7.5, color=C["mid"])

    # ── Gold column ───────────────────────────────────────────────────────────
    _box(ax, 17.15, COL_Y + 0.45, 4.4, 5.0, fill=C["white"], edge=C["gold"],
         radius=0.15, lw=1.5)
    _txt(ax, 19.35, COL_Y + 5.15, "GoldBuilder", size=10, bold=True, color=C["gold"])
    gd_lines = [
        "• Lê tabelas Silver",
        "• orders ⋈ shipments ⋈ items",
        "• Agrega itens por pedido",
        "• KPIs Revenue:",
        "  - gross/net/discount_pct",
        "  - avg_unit_price",
        "• KPIs Entrega:",
        "  - delivery_time_hours / is_late",
        "  - days_to_ship / is_delivered",
        "• Temporal: year_month / weekday",
        "• Validação Pandera estrita",
    ]
    for j, line in enumerate(gd_lines):
        _txt(ax, 17.35, COL_Y + 4.8 - j * 0.42, line, size=8, ha="left", color=C["dark"])

    _box(ax, 17.15, COL_Y + 5.8, 4.4, 3.1, fill=C["white"], edge=C["gold"],
         radius=0.15, lw=1.5)
    _txt(ax, 19.35, COL_Y + 8.55, "Saída — data/gold/", size=9, bold=True, color=C["gold"])
    gold_tables = [
        ("fact_orders",      "1 linha/pedido · 30 cols"),
        ("fact_order_items", "1 linha/item · 8 cols"),
        ("dim_customers",    "1 linha/cliente · 4 cols"),
        ("dim_products",     "1 linha/produto · 4 cols"),
    ]
    for k, (tname, desc) in enumerate(gold_tables):
        by = COL_Y + 8.15 - k * 0.55
        _txt(ax, 17.35, by, f"• {tname}", size=8.5, bold=True, ha="left", color=C["gold"])
        _txt(ax, 21.5, by, desc, size=7.5, ha="right", color=C["mid"])

    # ── Inter-layer arrows (centred vertically) ───────────────────────────────
    mid_y = COL_Y + COL_H / 2
    for x1, x2 in [(5.35, 5.85), (10.85, 11.35), (16.35, 16.85)]:
        _arrow(ax, x1, mid_y, x2, mid_y, lw=2.8)

    # ── Footer ────────────────────────────────────────────────────────────────
    _txt(ax, 11, 0.28,
         "Validação: Pandera  |  Conectores: CSV · API · SQLAlchemy  |  Orquestração: main.py  |  Python 3.14 · Pandas 3.x",
         size=8, color=C["light"])

    _save(fig, "pipeline_architecture")


# ─────────────────────────────────────────────────────────────────────────────
# DIAGRAM 2 — Gold Star Schema
# ─────────────────────────────────────────────────────────────────────────────

def build_star_schema():
    """Star-schema diagram.

    Layout
    ------
    dim_customers (left)  ←→  fact_orders (centre)
                               ↕  order_id
                          fact_order_items (bottom-centre)  ←→  dim_products (bottom-right)

    All relationship arrows are horizontal or near-vertical so they are easy
    to follow.  Cardinality (N:1) is shown on each arrow label.
    """
    fig, ax = plt.subplots(figsize=(22, 16))
    fig.patch.set_facecolor(C["bg"])
    ax.set_facecolor(C["bg"])
    ax.set_xlim(0, 22)
    ax.set_ylim(0, 16)
    ax.axis("off")

    # ── Title ────────────────────────────────────────────────────────────────
    _txt(ax, 11, 15.55, "Modelo Estrela — Camada Gold",
         size=17, bold=True, color=C["dark"])
    _txt(ax, 11, 15.1,
         "E-commerce Analytics  ·  fact_orders  ·  fact_order_items  ·  dim_customers  ·  dim_products",
         size=10, color=C["mid"])

    # ── Box positions ─────────────────────────────────────────────────────────
    # fact_orders: tall centre box
    FX,  FY,  FW,  FH  = 4.6,  3.8, 9.5, 10.5
    # dim_customers: left, vertically centred on the upper half of fact_orders
    DCX, DCY, DCW, DCH = 0.2,  9.2, 4.0,  4.0
    # fact_order_items: directly below fact_orders, same x-extent
    FIX, FIY, FIW, FIH = 4.6,  0.5, 9.5,  2.9
    # dim_products: bottom-right, same height as fact_order_items
    DPX, DPY, DPW, DPH = 15.3, 0.5, 5.8,  3.8

    # ── FACT_ORDERS ───────────────────────────────────────────────────────────
    _box(ax, FX, FY, FW, FH, fill=C["fact_lt"], edge=C["fact"], radius=0.3, lw=2.5)
    _hdr(ax, FX, FY + FH - 0.75, FW, 0.75, fill=C["fact"],
         label="fact_orders  (grain: 1 linha por pedido)", size=11)

    fact_sections = [
        ("── Keys ─────────────────────────────────────────────────────", [
            "order_id  PK",
            "customer_id  FK → dim_customers",
        ]),
        ("── Temporal ──────────────────────────────────────────────────", [
            "order_date  ·  order_ts",
            "order_year  ·  order_month  ·  order_year_month  ·  order_weekday",
        ]),
        ("── Revenue ───────────────────────────────────────────────────", [
            "gross_amount  ·  discount_total  ·  discount_pct",
            "net_amount  ·  items_count  ·  total_units  ·  avg_unit_price",
        ]),
        ("── Payment & Status ──────────────────────────────────────────", [
            "payment_method  ·  status_final",
        ]),
        ("── Shipping ──────────────────────────────────────────────────", [
            "carrier  ·  shipping_cost  ·  shipping_pct_net",
            "shipped_date  ·  shipped_ts  ·  delivered_date  ·  delivered_ts",
        ]),
        ("── Delivery KPIs ─────────────────────────────────────────────", [
            "days_to_ship  ·  delivery_time_hours",
            "is_late  ·  is_delivered  ·  is_returned  ·  delivery_status",
        ]),
    ]

    row_y = FY + FH - 1.1
    LINE_H = 0.44
    for sec_label, rows in fact_sections:
        _txt(ax, FX + 0.3, row_y, sec_label, size=7.5, bold=True,
             ha="left", color=C["fact"])
        row_y -= LINE_H * 0.85
        for r in rows:
            _txt(ax, FX + 0.45, row_y, r, size=8.5,
                 ha="left", color=C["dark"])
            row_y -= LINE_H

    # ── DIM_CUSTOMERS (left) ──────────────────────────────────────────────────
    _box(ax, DCX, DCY, DCW, DCH, fill=C["dim_lt"], edge=C["dim"], radius=0.22, lw=2.2)
    _hdr(ax, DCX, DCY + DCH - 0.68, DCW, 0.68, fill=C["dim"],
         label="dim_customers", size=11)
    for i, (col, pk) in enumerate([
        ("customer_id  PK", True),
        ("state  (UF uppercase)", False),
        ("city   (Title Case)", False),
        ("created_ts  (datetime)", False),
    ]):
        _txt(ax, DCX + 0.22, DCY + DCH - 1.18 - i * 0.68, col,
             size=9, bold=pk, ha="left",
             color=C["dim"] if pk else C["dark"])

    # ── FACT_ORDER_ITEMS (bottom-centre) ──────────────────────────────────────
    _box(ax, FIX, FIY, FIW, FIH, fill=C["fact_lt"], edge=C["fact"], radius=0.22, lw=2.2)
    _hdr(ax, FIX, FIY + FIH - 0.65, FIW, 0.65, fill=C["fact"],
         label="fact_order_items  (grain: 1 linha por pedido + produto)", size=10)
    for i, (col, pk) in enumerate([
        ("order_id   PK, FK → fact_orders",                True),
        ("product_id PK, FK → dim_products",               True),
        ("quantity  ·  unit_price  ·  gross_item_amount",   False),
        ("discount_amount  ·  discount_pct  ·  item_net_amount", False),
    ]):
        _txt(ax, FIX + 0.3, FIY + FIH - 1.05 - i * 0.55, col,
             size=9, bold=pk, ha="left",
             color=C["fact"] if pk else C["dark"])

    # ── DIM_PRODUCTS (bottom-right) ───────────────────────────────────────────
    _box(ax, DPX, DPY, DPW, DPH, fill=C["dim_lt"], edge=C["dim"], radius=0.22, lw=2.2)
    _hdr(ax, DPX, DPY + DPH - 0.68, DPW, 0.68, fill=C["dim"],
         label="dim_products", size=11)
    for i, (col, pk) in enumerate([
        ("product_id  PK", True),
        ("category  (Title Case)", False),
        ("brand  (Title Case)", False),
        ("created_ts  (datetime)", False),
    ]):
        _txt(ax, DPX + 0.22, DPY + DPH - 1.18 - i * 0.68, col,
             size=9, bold=pk, ha="left",
             color=C["dim"] if pk else C["dark"])

    # ── Relationship arrows ───────────────────────────────────────────────────
    arrow_kw = dict(color=C["dim"], lw=2.2)

    # 1. dim_customers → fact_orders  (horizontal, N:1)
    dc_mid_y = DCY + DCH * 0.35          # midpoint on dim_customers right edge
    fo_left_y = FY + FH * 0.87          # matching point on fact_orders left edge
    _arrow(ax, DCX + DCW, dc_mid_y, FX, fo_left_y, **arrow_kw)
    _txt(ax, (DCX + DCW + FX) / 2, dc_mid_y + 0.35,
         "customer_id  (N : 1)", size=8.5, bold=True, color=C["dim"])

    # 2. dim_products → fact_order_items  (horizontal, N:1)
    dp_mid_y = DPY + DPH * 0.42          # midpoint on dim_products left edge
    fi_right_y = FIY + FIH * 0.42       # matching point on fact_order_items right edge
    _arrow(ax, DPX, dp_mid_y, FIX + FIW, fi_right_y, **arrow_kw)
    _txt(ax, (DPX + FIX + FIW) / 2, dp_mid_y + 0.35,
         "product_id  (N : 1)", size=8.5, bold=True, color=C["dim"])

    # 3. fact_order_items → fact_orders  (vertical, N:1)
    fi_cx = FIX + FIW / 2
    _arrow(ax, fi_cx, FIY + FIH, fi_cx, FY, color=C["fact"], lw=2.2)
    _txt(ax, fi_cx + 1.5, FIY + FIH + 0.18,
         "order_id  (N : 1)", size=8.5, bold=True, color=C["fact"])

    # ── Legend ────────────────────────────────────────────────────────────────
    lx = 0.4
    for fc, ec, label in [
        (C["fact_lt"], C["fact"], "Tabela Fato"),
        (C["dim_lt"],  C["dim"],  "Tabela Dimensão"),
    ]:
        _box(ax, lx, 15.0, 0.38, 0.3, fill=fc, edge=ec, radius=0.05, lw=1.5)
        _txt(ax, lx + 0.56, 15.15, label, size=9, ha="left", color=C["dark"])
        lx += 3.5

    _txt(ax, 11, 0.22,
         "PK = Chave Primária  |  FK = Chave Estrangeira  |  Grain: nível de detalhe da tabela",
         size=8.5, color=C["light"])

    _save(fig, "star_schema")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Generating diagrams...")
    build_pipeline_diagram()
    build_star_schema()
    print("Done.")
