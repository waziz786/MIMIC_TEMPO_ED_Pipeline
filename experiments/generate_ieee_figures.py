"""
IEEE-Quality Figure Generation
================================
Generates publication-ready figures for the IEEE manuscript.

Figures:
  Fig. 1  — Pipeline architecture diagram (simplified block diagram)
  Fig. 2  — Information gain across observation windows (line plot with CI)
  Fig. 3  — Multi-outcome AUROC comparison (horizontal bar chart)
  Fig. 4  — ECG incremental value (paired bar chart with delta annotations)
  Fig. 5  — Leakage demonstration (grouped bar chart with zones)

All saved as PDF + PNG (300 dpi) in artifacts/results/figures/
"""

import json, warnings, textwrap
from pathlib import Path

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.ticker import FormatStrFormatter

warnings.filterwarnings("ignore")

OUT  = Path("artifacts/results")
FIGS = OUT / "figures"; FIGS.mkdir(parents=True, exist_ok=True)

# ── IEEE Style ──────────────────────────────────────────────────────────────
plt.rcParams.update({
    'font.size':        9,
    'font.family':      'serif',
    'font.serif':       ['Times New Roman', 'DejaVu Serif', 'serif'],
    'mathtext.fontset': 'dejavuserif',
    'axes.titlesize':   10,
    'axes.titleweight': 'bold',
    'axes.labelsize':   9,
    'axes.linewidth':   0.6,
    'xtick.labelsize':  8,
    'ytick.labelsize':  8,
    'xtick.major.width':0.5,
    'ytick.major.width':0.5,
    'legend.fontsize':  8,
    'legend.framealpha': 0.9,
    'legend.edgecolor': '0.8',
    'figure.dpi':       300,
    'savefig.bbox':     'tight',
    'savefig.pad_inches': 0.03,
    'lines.linewidth':  1.3,
    'lines.markersize': 5,
    'grid.alpha':       0.25,
    'grid.linewidth':   0.4,
})

# Color palette — colorblind-friendly
C_LR  = '#2166AC'   # blue
C_XGB = '#B2182B'   # red
C_BG  = '#F7F7F7'


def load_json(fname):
    p = OUT / fname
    if not p.exists():
        print(f"  ✗ {fname} not found"); return None
    with open(p) as f:
        return json.load(f)


def save(fig, name):
    for ext in ['pdf', 'png']:
        fig.savefig(FIGS / f"{name}.{ext}", dpi=300)
    plt.close(fig)
    print(f"  ✅ {name}")


# ============================================================================
# Fig. 1 — Pipeline Architecture Diagram
# ============================================================================
def fig1_pipeline():
    """Simplified block-diagram of the 6-stage pipeline."""
    fig, ax = plt.subplots(figsize=(3.4, 4.0))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 12)
    ax.axis('off')

    stages = [
        ("MIMIC-IV\n(ED · Hosp · ICU · ECG)", 10.5, '#E0E0E0', 'black'),
        ("Stage 1: Base ED Cohort\n424,952 adult visits",         9.0, '#DEEBF7', C_LR),
        ("Stage 2: Unified Event Log\n9 event types, 82K events", 7.5, '#DEEBF7', C_LR),
        ("Stage 3: Outcome Derivation\n20+ binary & time-to-event",6.0, '#DEEBF7', C_LR),
        ("Stage 4: Feature Windows\nW1 (18) · W6 (50) · W24 (59)",4.5, '#DEEBF7', C_LR),
        ("Stage 5: ECG Integration\n15 waveform features",        3.0, '#FEE0D2', C_XGB),
        ("Stage 6: Dataset Materialization\nCSV + QA validation",  1.5, '#D9F0D3', '#1B7837'),
    ]

    box_w, box_h = 8, 1.1
    for label, y, facecolor, edgecolor in stages:
        rect = mpatches.FancyBboxPatch(
            (1, y - box_h/2), box_w, box_h,
            boxstyle="round,pad=0.15",
            facecolor=facecolor, edgecolor=edgecolor, linewidth=0.8
        )
        ax.add_patch(rect)
        ax.text(5, y, label, ha='center', va='center', fontsize=7,
                fontweight='bold' if y > 10 else 'normal')

    # Arrows between stages
    for i in range(len(stages) - 1):
        y_from = stages[i][1] - box_h/2 - 0.05
        y_to   = stages[i+1][1] + box_h/2 + 0.05
        ax.annotate('', xy=(5, y_to), xytext=(5, y_from),
                     arrowprops=dict(arrowstyle='->', color='0.4', lw=0.8))

    ax.set_title("Fig. 1. Pipeline architecture overview.", fontsize=9,
                  fontweight='normal', style='italic', pad=5)
    fig.tight_layout()
    save(fig, "fig1_pipeline")


# ============================================================================
# Fig. 2 — Information Gain (Line Plot with CI bands)
# ============================================================================
def fig2_information_gain(data):
    """AUROC vs window for both cohorts, line plot with CI ribbons."""
    if data is None: return

    fig, axes = plt.subplots(1, 2, figsize=(6.8, 2.6), sharey=True)
    windows = ["W1", "W6", "W24"]
    x = np.arange(len(windows))

    for ax, (cohort, title) in zip(axes,
            [("all_ed", "(a) All ED Visits (N=75,000)"),
             ("admitted", "(b) Admitted Only (N=35,977)")]):
        cdata = data.get(cohort, {})
        if not cdata: continue

        ax.set_facecolor(C_BG)

        for mdl, color, marker in [("LR", C_LR, 's'), ("XGB", C_XGB, 'o')]:
            means, lo, hi = [], [], []
            for w in windows:
                d = cdata.get(w, {}).get(mdl, {})
                m = d.get("auroc_mean", np.nan)
                ci = d.get("auroc_ci", [m, m])
                means.append(m)
                lo.append(ci[0])
                hi.append(ci[1])

            ax.plot(x, means, marker=marker, color=color, label=mdl,
                    zorder=3, markersize=6)
            ax.fill_between(x, lo, hi, color=color, alpha=0.12, zorder=2)

            # Value labels
            for xi, m in zip(x, means):
                ax.annotate(f'{m:.3f}', xy=(xi, m),
                            xytext=(0, 8), textcoords='offset points',
                            ha='center', fontsize=6.5, color=color,
                            fontweight='bold')

        ax.set_xticks(x)
        ax.set_xticklabels(["1 h", "6 h", "24 h"])
        ax.set_xlabel("Observation Window")
        ax.set_title(title, fontsize=9)
        ax.grid(True, axis='both', zorder=0)
        ax.set_ylim(0.82, 0.98)

    axes[0].set_ylabel("AUROC (95% CI)")
    axes[1].legend(loc='lower right', frameon=True)

    fig.suptitle("Fig. 2. Temporal information gain for deterioration_24h prediction.",
                 fontsize=9, style='italic', y=-0.02)
    fig.tight_layout()
    save(fig, "fig2_information_gain")


# ============================================================================
# Fig. 3 — Multi-Outcome Comparison (Horizontal Grouped Bar)
# ============================================================================
def fig3_multi_outcome(data):
    """Horizontal grouped bar chart of AUROC across outcomes."""
    if data is None: return

    # Ordered by XGB AUROC descending
    outcomes_order = ["pressor_24h", "death_24h", "vent_24h",
                      "icu_24h", "deterioration_24h"]
    labels = {
        "deterioration_24h": "Composite\ndeterioration",
        "icu_24h":           "ICU\nadmission",
        "death_24h":         "Death\n(24 h)",
        "vent_24h":          "Mechanical\nventilation",
        "pressor_24h":       "Vasopressor\ninitiation"
    }

    fig, ax = plt.subplots(figsize=(3.4, 2.8))
    ax.set_facecolor(C_BG)

    y = np.arange(len(outcomes_order))
    height = 0.35

    for i, (mdl, color) in enumerate([("LR", C_LR), ("XGB", C_XGB)]):
        vals = [data[o].get(mdl, {}).get("auroc_mean", 0) for o in outcomes_order]
        ci_lo = [data[o].get(mdl, {}).get("auroc_ci", [0,0])[0] for o in outcomes_order]
        ci_hi = [data[o].get(mdl, {}).get("auroc_ci", [0,0])[1] for o in outcomes_order]
        xerr_lo = [v - l for v, l in zip(vals, ci_lo)]
        xerr_hi = [h - v for v, h in zip(vals, ci_hi)]

        bars = ax.barh(y + (i - 0.5) * height, vals, height,
                       xerr=[xerr_lo, xerr_hi],
                       label=mdl, color=color, alpha=0.85,
                       capsize=2, error_kw={'linewidth': 0.6})

        for bar, v in zip(bars, vals):
            ax.text(v + 0.003, bar.get_y() + bar.get_height()/2,
                    f'{v:.3f}', va='center', fontsize=6.5, color=color,
                    fontweight='bold')

    ax.set_yticks(y)
    ax.set_yticklabels([labels[o] for o in outcomes_order], fontsize=7)
    ax.set_xlabel("AUROC (95% CI)")
    ax.set_xlim(0.85, 0.97)
    ax.legend(loc='lower right', frameon=True)
    ax.grid(True, axis='x', zorder=0)
    ax.invert_yaxis()

    ax.set_title("Fig. 3. Multi-outcome evaluation\n(W6 features, admitted cohort).",
                 fontsize=9, style='italic')
    fig.tight_layout()
    save(fig, "fig3_multi_outcome")


# ============================================================================
# Fig. 4 — ECG Incremental Value (Paired Bar with Delta)
# ============================================================================
def fig4_ecg_delta(data):
    """Paired bar chart: clinical-only vs clinical+ECG, cardiac outcomes."""
    if data is None: return

    outcomes = list(data.keys())
    if not outcomes: return

    fig, axes = plt.subplots(1, 2, figsize=(6.8, 2.4), sharey=False)
    labels_map = {
        "cardiac_arrest_hosp": "Cardiac Arrest",
        "acs_hosp": "ACS"
    }

    for ax, mdl, color in zip(axes, ["LR", "XGB"], [C_LR, C_XGB]):
        ax.set_facecolor(C_BG)
        oc_labels = []
        clin_vals, ecg_vals = [], []

        for oc in outcomes:
            oc_labels.append(labels_map.get(oc, oc))
            clin_vals.append(data[oc]["clinical"][mdl]["auroc_mean"])
            ecg_vals.append(data[oc]["clinical_ecg"][mdl]["auroc_mean"])

        x = np.arange(len(oc_labels))
        w = 0.3

        bars1 = ax.bar(x - w/2, clin_vals, w, label='Clinical only',
                        color=color, alpha=0.45, edgecolor=color, linewidth=0.8)
        bars2 = ax.bar(x + w/2, ecg_vals, w, label='+ ECG',
                        color=color, alpha=0.90, edgecolor=color, linewidth=0.8)

        # Delta annotations
        for xi, c, e in zip(x, clin_vals, ecg_vals):
            delta = e - c
            ax.annotate(f'Δ={delta:+.003f}',
                        xy=(xi + w/2, e), xytext=(0, 6),
                        textcoords='offset points',
                        ha='center', fontsize=7, color='black',
                        fontweight='bold',
                        arrowprops=dict(arrowstyle='-', color='0.5', lw=0.5))

        # Value labels on bars
        for bars in [bars1, bars2]:
            for bar in bars:
                h = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2, h - 0.012,
                        f'{h:.3f}', ha='center', va='top', fontsize=6,
                        color='white', fontweight='bold')

        ax.set_xticks(x)
        ax.set_xticklabels(oc_labels, fontsize=8)
        ax.set_ylabel("AUROC")
        ax.set_title(f"({['a','b'][['LR','XGB'].index(mdl)]}) {mdl}", fontsize=9)
        ax.legend(loc='upper left', fontsize=7)
        ax.set_ylim(0.75, 0.95)
        ax.grid(True, axis='y', zorder=0)

    fig.suptitle("Fig. 4. ECG incremental value for cardiac outcomes (W6, admitted).",
                 fontsize=9, style='italic', y=-0.02)
    fig.tight_layout()
    save(fig, "fig4_ecg_delta")


# ============================================================================
# Fig. 5 — Leakage Demonstration
# ============================================================================
def fig5_leakage(data):
    """Grouped bar chart with 4 conditions + CI error bars + zone shading."""
    if data is None: return

    order = [
        ("leakage_free_w6",  "Leakage-free\n(W6)"),
        ("naive1_temporal",   "Naive-1\n(Temporal)"),
        ("naive2_process",    "Naive-2\n(Process)"),
        ("negative_control",  "Negative\nControl"),
    ]

    labels, lr_vals, xgb_vals = [], [], []
    lr_lo, lr_hi, xgb_lo, xgb_hi = [], [], [], []

    for key, lbl in order:
        d = data.get(key, {})
        if not d: continue
        labels.append(lbl)
        lr  = d.get("LR", {})
        xgb = d.get("XGB", {})
        lr_m  = lr.get("auroc_mean", 0)
        xgb_m = xgb.get("auroc_mean", 0)
        lr_vals.append(lr_m)
        xgb_vals.append(xgb_m)
        lr_ci  = lr.get("auroc_ci", [lr_m, lr_m])
        xgb_ci = xgb.get("auroc_ci", [xgb_m, xgb_m])
        lr_lo.append(lr_m - lr_ci[0])
        lr_hi.append(lr_ci[1] - lr_m)
        xgb_lo.append(xgb_m - xgb_ci[0])
        xgb_hi.append(xgb_ci[1] - xgb_m)

    if not labels: return

    fig, ax = plt.subplots(figsize=(3.4, 3.0))
    ax.set_facecolor(C_BG)
    x = np.arange(len(labels))
    w = 0.32

    ax.bar(x - w/2, lr_vals, w, yerr=[lr_lo, lr_hi],
           label='LR', color=C_LR, alpha=0.85, capsize=2,
           error_kw={'linewidth': 0.6})
    ax.bar(x + w/2, xgb_vals, w, yerr=[xgb_lo, xgb_hi],
           label='XGB', color=C_XGB, alpha=0.85, capsize=2,
           error_kw={'linewidth': 0.6})

    # Value annotations
    for xi, lv, xv in zip(x, lr_vals, xgb_vals):
        ax.text(xi - w/2, lv + 0.018, f'{lv:.3f}', ha='center',
                fontsize=6, color=C_LR, fontweight='bold')
        ax.text(xi + w/2, xv + 0.018, f'{xv:.3f}', ha='center',
                fontsize=6, color=C_XGB, fontweight='bold')

    # Inflation delta annotations for Naive-1 and Naive-2
    if len(lr_vals) >= 3:
        for i in [1, 2]:
            d_lr  = lr_vals[i]  - lr_vals[0]
            d_xgb = xgb_vals[i] - xgb_vals[0]
            y_pos = max(lr_vals[i], xgb_vals[i]) + 0.04
            txt = f'Δ LR +{d_lr:.1%}\nΔ XGB +{d_xgb:.1%}'
            ax.text(x[i], y_pos, txt, ha='center', fontsize=5.5,
                    color='#8B0000', fontweight='bold',
                    bbox=dict(boxstyle='round,pad=0.2', facecolor='#FFF0F0',
                              edgecolor='#CC0000', alpha=0.8, linewidth=0.5))

    # Zone shading
    ax.axvspan(-0.5, 0.5, alpha=0.08, color='green', zorder=0)
    ax.axvspan(0.5, 2.5, alpha=0.08, color='red', zorder=0)
    ax.axvspan(2.5, 3.5, alpha=0.08, color='gray', zorder=0)

    # Chance line
    ax.axhline(0.5, color='0.5', linewidth=0.7, linestyle='--', zorder=1)
    ax.text(3.3, 0.51, 'chance', fontsize=6, color='0.5', ha='right')

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=7)
    ax.set_ylabel("AUROC (95% CI)")
    ax.set_ylim(0.40, 1.12)
    ax.legend(loc='upper left', frameon=True)
    ax.grid(True, axis='y', zorder=0)

    ax.set_title("Fig. 5. Leakage demonstration\n(deterioration_24h, admitted cohort).",
                 fontsize=9, style='italic')
    fig.tight_layout()
    save(fig, "fig5_leakage")


# ============================================================================
# MAIN
# ============================================================================
def main():
    print("Loading results …")
    opt_a = load_json("option_a_results.json")
    opt_b = load_json("option_b_leakage.json")

    print("\nGenerating IEEE figures …\n")

    fig1_pipeline()

    if opt_a:
        fig2_information_gain(opt_a.get("A1_multi_window"))
        fig3_multi_outcome(opt_a.get("A2_multi_outcome"))
        fig4_ecg_delta(opt_a.get("A3_ecg_cardiac"))

    if opt_b:
        fig5_leakage(opt_b)

    print(f"\n✅ All IEEE figures saved to {FIGS}")


if __name__ == "__main__":
    main()
