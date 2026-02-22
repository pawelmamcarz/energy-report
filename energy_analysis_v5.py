#!/usr/bin/env python3
"""Energy cost analysis v5 — PGE hourly netted data + real tariff rates.

Changes from v4:
- PRIMARY DATA SOURCE: PGE CSV with hourly netted values ("En. Czynna zbilansowana")
  → Exact peak vs offpeak split per G12w tariff
  → Exact import/export per hour (after PGE's hourly netting)
- REAL TARIFF RATES from PGE invoices (not estimates!)
  → Variable rates differ by component (energy, distribution, quality, OZE, cogen)
  → Peak vs offpeak difference is ONLY in distribution fee (sieciowa zmienna)
  → Fixed monthly fees included (~94 PLN brutto/month)
- NM (net-metering) modeled per-zone with cross-zone transfer
- All costs in PLN brutto (with 23% VAT)
- Validated against actual PGE invoices (Aug, Oct, Nov, Dec 2025)

Data sources:
- PGE CSV: Feb 2025 - Jan 2026 (hourly netted, zbilansowana)
- Feb 2026: Zamel HA (extrapolated to full month)
- PV: Fusion Solar portal + HA
- Load/HP/Tesla: HA sensors + estimates for pre-Aug
"""
import csv
import json
from datetime import datetime, date
from collections import defaultdict

# ============================================================
# CONFIG: Real PGE tariff rates (from Dec 2025 invoice)
# ============================================================
# Variable rates (PLN netto per kWh)
# Note: Energy price varied slightly by month in 2025 due to tariff changes
# Using Dec 2025 rates as baseline (most recent and representative)
ENERGIA_NETTO = 0.51677        # Same for peak and offpeak!
SIECIOWA_PEAK_NETTO = 0.42760  # Distribution fee - daytime
SIECIOWA_OFFPEAK_NETTO = 0.08450  # Distribution fee - nighttime
JAKOSCIOWA_NETTO = 0.03210
OZE_NETTO = 0.00350
KOGENERACYJNA_NETTO = 0.00300

# Total variable rates (netto)
PEAK_RATE_NETTO = ENERGIA_NETTO + SIECIOWA_PEAK_NETTO + JAKOSCIOWA_NETTO + OZE_NETTO + KOGENERACYJNA_NETTO
OFFPEAK_RATE_NETTO = ENERGIA_NETTO + SIECIOWA_OFFPEAK_NETTO + JAKOSCIOWA_NETTO + OZE_NETTO + KOGENERACYJNA_NETTO

VAT = 1.23
PEAK_RATE = PEAK_RATE_NETTO * VAT      # ~1.209 PLN brutto
OFFPEAK_RATE = OFFPEAK_RATE_NETTO * VAT  # ~0.787 PLN brutto

# Fixed monthly fees (PLN brutto)
FIXED_MONTHLY = {
    'handlowa': 49.90,
    'sieciowa_stala': 18.43,
    'przejsciowa': 0.41,
    'mocowa': 19.69,
    'abonament': 5.54,
}
FIXED_MONTHLY_TOTAL = sum(FIXED_MONTHLY.values())  # ~93.97 PLN

# NM (net-metering) parameters
NM_RATIO = 0.80  # Old prosument ≤10kW: 80% return
NM_MONTHS = 12   # Rolling 12-month credit period

# Other
GAS_RATE = 0.35   # PLN/kWh thermal equivalent for gas comparison

print(f"=== Stawki PGE (z faktury grudniowej 2025) ===")
print(f"  Stawka szczytowa (peak):    {PEAK_RATE_NETTO:.5f} netto → {PEAK_RATE:.3f} brutto PLN/kWh")
print(f"  Stawka pozaszczytowa (off): {OFFPEAK_RATE_NETTO:.5f} netto → {OFFPEAK_RATE:.3f} brutto PLN/kWh")
print(f"  Opłaty stałe miesięczne:    {FIXED_MONTHLY_TOTAL:.2f} PLN brutto")
print(f"  Różnica peak vs offpeak to TYLKO opłata sieciowa zmienna!")
print(f"    Peak:    {SIECIOWA_PEAK_NETTO:.5f} vs Offpeak: {SIECIOWA_OFFPEAK_NETTO:.5f} netto")

# ============================================================
# PART 1: Parse PGE CSV — hourly netted data
# ============================================================
pge_csv_path = "/sessions/awesome-relaxed-franklin/mnt/uploads/202602221445_590543570101381668_7acfae01-11aa-45c2-805b-e25a98296b94.csv"

# G12w tariff zones (PGE Dystrybucja):
#
# WINTER (1 Oct - 31 Mar):
#   Weekday peak:    06:00-13:00, 15:00-22:00  → H07-H13, H16-H22
#   Weekday offpeak: 13:00-15:00, 22:00-06:00  → H14-H15, H23-H24, H01-H06
#   Weekend/holiday: ENTIRE DAY offpeak         → H01-H24
#
# SUMMER (1 Apr - 30 Sep):
#   Weekday peak:    06:00-15:00, 17:00-22:00  → H07-H15, H18-H22
#   Weekday offpeak: 15:00-17:00, 22:00-06:00  → H16-H17, H23-H24, H01-H06
#   Weekend/holiday: ENTIRE DAY offpeak         → H01-H24
#
# CSV columns: H01=00:00-01:00, H02=01:00-02:00, ..., H24=23:00-24:00
#
# Polish public holidays 2025-2026 (treated as offpeak all day):
POLISH_HOLIDAYS = {
    date(2025, 1, 1), date(2025, 1, 6),   # Nowy Rok, Trzech Króli
    date(2025, 4, 20), date(2025, 4, 21),  # Wielkanoc
    date(2025, 5, 1), date(2025, 5, 3),    # Święto Pracy, Konstytucja
    date(2025, 6, 8),                       # Zesłanie Ducha Św.
    date(2025, 6, 19),                      # Boże Ciało
    date(2025, 8, 15),                      # Wniebowzięcie NMP
    date(2025, 11, 1), date(2025, 11, 11),  # Wszystkich Świętych, Niepodległość
    date(2025, 12, 25), date(2025, 12, 26), # Boże Narodzenie
    date(2026, 1, 1), date(2026, 1, 6),     # Nowy Rok, Trzech Króli
    date(2026, 2, 22),                       # (not a holiday, placeholder removed)
}

# Winter offpeak weekday hours: H01-H06, H14-H15, H23-H24
WINTER_OFFPEAK_WEEKDAY = {1, 2, 3, 4, 5, 6, 14, 15, 23, 24}
# Summer offpeak weekday hours: H01-H06, H16-H17, H23-H24
SUMMER_OFFPEAK_WEEKDAY = {1, 2, 3, 4, 5, 6, 16, 17, 23, 24}

def is_summer(d):
    """Check if date is in summer period (Apr 1 - Sep 30)."""
    return 4 <= d.month <= 9

def is_offpeak(hour_num, day_date):
    """Check if hour H{hour_num} on given date is offpeak under G12w."""
    # Weekends and public holidays: entire day is offpeak
    if day_date.weekday() >= 5 or day_date in POLISH_HOLIDAYS:
        return True
    # Weekday: depends on summer/winter schedule
    if is_summer(day_date):
        return hour_num in SUMMER_OFFPEAK_WEEKDAY
    else:
        return hour_num in WINTER_OFFPEAK_WEEKDAY

# Parse PGE data
pge_daily = []  # list of dicts with date, peak_import, offpeak_import, peak_export, offpeak_export

with open(pge_csv_path, 'r') as f:
    reader = csv.reader(f, delimiter=';')
    header = next(reader)

    for row in reader:
        kierunek = row[2].strip()
        if kierunek != "En. Czynna zbilansowana":
            continue

        date_str = row[1].strip()
        try:
            day_date = datetime.strptime(date_str, "%Y%m%d").date()
        except ValueError:
            continue

        peak_import = 0.0
        offpeak_import = 0.0
        peak_export = 0.0
        offpeak_export = 0.0

        for h in range(1, 25):
            val_str = row[2 + h].strip().replace(',', '.')
            try:
                val = float(val_str)
            except ValueError:
                continue

            offpeak = is_offpeak(h, day_date)

            if val > 0:  # Net import (consumption > production)
                if offpeak:
                    offpeak_import += val
                else:
                    peak_import += val
            elif val < 0:  # Net export (production > consumption)
                if offpeak:
                    offpeak_export += abs(val)
                else:
                    peak_export += abs(val)

        pge_daily.append({
            'date': day_date,
            'month': day_date.strftime('%Y-%m'),
            'peak_import': peak_import,
            'offpeak_import': offpeak_import,
            'peak_export': peak_export,
            'offpeak_export': offpeak_export,
        })

# Aggregate by month
pge_monthly = defaultdict(lambda: {
    'peak_import': 0, 'offpeak_import': 0,
    'peak_export': 0, 'offpeak_export': 0,
    'days': 0
})

for d in pge_daily:
    m = d['month']
    pge_monthly[m]['peak_import'] += d['peak_import']
    pge_monthly[m]['offpeak_import'] += d['offpeak_import']
    pge_monthly[m]['peak_export'] += d['peak_export']
    pge_monthly[m]['offpeak_export'] += d['offpeak_export']
    pge_monthly[m]['days'] += 1

print(f"\n\n=== Dane PGE CSV — Import/Eksport per strefa (zbilansowane godzinowo) ===")
print(f"{'Miesiąc':<10} {'Dni':>4} {'Peak imp':>10} {'Off imp':>10} {'IMPORT':>10} {'Peak exp':>10} {'Off exp':>10} {'EKSPORT':>10} {'%offpeak':>9}")
print("-" * 95)

for m in sorted(pge_monthly.keys()):
    d = pge_monthly[m]
    total_imp = d['peak_import'] + d['offpeak_import']
    total_exp = d['peak_export'] + d['offpeak_export']
    offpeak_pct = d['offpeak_import'] / total_imp * 100 if total_imp > 0 else 0
    print(f"{m:<10} {d['days']:>4} {d['peak_import']:>10.1f} {d['offpeak_import']:>10.1f} {total_imp:>10.1f} {d['peak_export']:>10.1f} {d['offpeak_export']:>10.1f} {total_exp:>10.1f} {offpeak_pct:>8.1f}%")

# ============================================================
# PART 2: Fusion Solar PV (exact monthly, same as v4)
# ============================================================
fusion_solar_pv = {
    '2025-01': 256.38, '2025-02': 587.90, '2025-03': 1494.39,
    '2025-04': 2644.56, '2025-05': 2602.18, '2025-06': 2676.15,
    '2025-07': 2304.10, '2025-08': 2478.49, '2025-09': 1345.00,
    '2025-10': 546.30, '2025-11': 272.22, '2025-12': 153.83,
    '2026-01': 190.25, '2026-02': 297.09,
}

# ============================================================
# PART 3: Load, HP, Tesla data (same as v4)
# ============================================================
total_load_ha = {
    '2025-08': 2198.0, '2025-09': 1580.6, '2025-10': 2118.2,
    '2025-11': 3061.2, '2025-12': 3327.2, '2026-01': 4673.1, '2026-02': 2387.0
}
hp_elec = {
    '2025-08': 420.0, '2025-09': 118.8, '2025-10': 375.6,
    '2025-11': 965.8, '2025-12': 1366.9, '2026-01': 1726.5, '2026-02': 888.1
}
hp_thermal = {
    '2025-08': 900.0, '2025-09': 240.3, '2025-10': 1182.8,
    '2025-11': 2039.0, '2025-12': 3014.1, '2026-01': 3257.1, '2026-02': 1945.0
}
tesla_kwh = {
    '2025-08': 381.5, '2025-09': 520.4, '2025-10': 672.9,
    '2025-11': 1035.4, '2025-12': 919.6, '2026-01': 1146.3, '2026-02': 879.3
}

pre_aug_estimates = {
    '2025-03': {'load_est': 2280, 'hp_est': 600, 'hp_th_est': 1500, 'tesla_est': 0},
    '2025-04': {'load_est': 2300, 'hp_est': 300, 'hp_th_est': 1000, 'tesla_est': 0},
    '2025-05': {'load_est': 2300, 'hp_est': 150, 'hp_th_est': 550, 'tesla_est': 250},
    '2025-06': {'load_est': 1935, 'hp_est': 100, 'hp_th_est': 400, 'tesla_est': 300},
    '2025-07': {'load_est': 1736, 'hp_est': 80, 'hp_th_est': 320, 'tesla_est': 350},
}

# ============================================================
# PART 4: Build monthly analysis (Mar 2025 - Feb 2026)
# ============================================================
analysis_months = ['2025-03', '2025-04', '2025-05', '2025-06', '2025-07',
                   '2025-08', '2025-09', '2025-10', '2025-11', '2025-12',
                   '2026-01', '2026-02']

labels = {'2025-03': 'Mar 25', '2025-04': 'Kwi 25', '2025-05': 'Maj 25',
          '2025-06': 'Cze 25', '2025-07': 'Lip 25', '2025-08': 'Sie 25',
          '2025-09': 'Wrz 25', '2025-10': 'Paź 25', '2025-11': 'Lis 25',
          '2025-12': 'Gru 25', '2026-01': 'Sty 26', '2026-02': 'Lut 26*'}

annual_pv = sum(fusion_solar_pv[m] for m in analysis_months)

print(f"\n\n{'='*150}")
print(f"  ANALIZA KOSZTÓW ENERGETYCZNYCH — JESIONOWA (v5: PGE dane godzinowe + realne stawki)")
print(f"  Dane: Marzec 2025 – Luty 2026 (12 miesięcy)")
print(f"  PV roczne: {annual_pv/1000:.2f} MWh (Fusion Solar portal)")
print(f"  Stawki: peak {PEAK_RATE:.3f} / offpeak {OFFPEAK_RATE:.3f} PLN brutto/kWh + stałe {FIXED_MONTHLY_TOTAL:.2f}/mies.")
print(f"{'='*150}")

# ============================================================
# PART 5: NM (Net-Metering) simulation per zone
# ============================================================
# NM works as follows:
# 1. Monthly export is recorded per zone (peak/offpeak)
# 2. Credits = export × 0.80 (per zone)
# 3. Credits applied: peak credits first to peak import, excess to offpeak
# 4. Credits valid 12 months, oldest used first
# 5. After NM, remaining import is billed at per-zone rates
#
# We simulate NM with a rolling credit bank.

print(f"\n\n{'='*150}")
print(f"  SYMULACJA NET-METERING (prosument na starych zasadach, ratio={NM_RATIO})")
print(f"{'='*150}")

# NM credit bank: list of (month_created, peak_credits, offpeak_credits)
nm_bank = []

# We need data BEFORE our analysis period to seed NM bank
# From Aug 2025 PGE invoice: Apr-Aug NM balance was tracked
# PGE invoices show the NM tracking table. Let me reconstruct:
# Apr: 163 peak + 201 offpeak export → 130.4 peak + 160.8 offpeak credits
# ... but this is complex. Let's start fresh from our data and calculate.

# Actually, from the PGE CSV we have Feb 2025 onwards.
# Let's process ALL months in the CSV for NM accumulation, then report on analysis months.

all_pge_months = sorted(pge_monthly.keys())

print(f"\n{'Miesiąc':<10} {'Peak imp':>9} {'Off imp':>9} {'Peak exp':>9} {'Off exp':>9} {'NM pk cr':>9} {'NM of cr':>9} {'NM użyte':>9} {'Rach peak':>10} {'Rach off':>10} {'RACHUNEK':>10} {'Stałe':>7} {'TOTAL':>10}")
print("-" * 150)

results = []
nm_peak_bank = 0.0    # Accumulated peak NM credits (kWh)
nm_offpeak_bank = 0.0  # Accumulated offpeak NM credits (kWh)

for m in all_pge_months:
    d = pge_monthly[m]
    peak_imp = d['peak_import']
    offpeak_imp = d['offpeak_import']
    peak_exp = d['peak_export']
    offpeak_exp = d['offpeak_export']
    total_imp = peak_imp + offpeak_imp
    total_exp = peak_exp + offpeak_exp

    # Generate new NM credits from this month's export
    new_peak_credits = peak_exp * NM_RATIO
    new_offpeak_credits = offpeak_exp * NM_RATIO

    # Add new credits to bank
    nm_peak_bank += new_peak_credits
    nm_offpeak_bank += new_offpeak_credits

    # Apply NM credits to this month's import
    # Step 1: Peak credits offset peak import
    peak_nm_used = min(nm_peak_bank, peak_imp)
    nm_peak_bank -= peak_nm_used
    peak_billed = peak_imp - peak_nm_used

    # Step 2: Excess peak credits transfer to offpeak
    peak_excess = nm_peak_bank  # any remaining peak credits
    # (In PGE's system, excess peak credits CAN offset offpeak import)

    # Step 3: Offpeak credits + excess peak credits offset offpeak import
    offpeak_nm_available = nm_offpeak_bank + peak_excess
    offpeak_nm_used = min(offpeak_nm_available, offpeak_imp)

    # Deduct from offpeak bank first, then from peak excess
    if offpeak_nm_used <= nm_offpeak_bank:
        nm_offpeak_bank -= offpeak_nm_used
    else:
        excess_used = offpeak_nm_used - nm_offpeak_bank
        nm_offpeak_bank = 0
        nm_peak_bank -= excess_used

    offpeak_billed = offpeak_imp - offpeak_nm_used

    total_nm_used = peak_nm_used + offpeak_nm_used

    # Calculate cost
    cost_peak = peak_billed * PEAK_RATE
    cost_offpeak = offpeak_billed * OFFPEAK_RATE
    cost_variable = cost_peak + cost_offpeak
    cost_fixed = FIXED_MONTHLY_TOTAL
    cost_total = cost_variable + cost_fixed

    in_analysis = m in analysis_months
    marker = " ←" if in_analysis else ""

    # PV, load, HP, Tesla for analysis months
    pv = fusion_solar_pv.get(m, 0)
    load = total_load_ha.get(m, 0)
    hp = hp_elec.get(m, 0)
    hp_th = hp_thermal.get(m, 0)
    tes = tesla_kwh.get(m, 0)

    if m in pre_aug_estimates:
        est = pre_aug_estimates[m]
        load = est['load_est']
        hp = est['hp_est']
        hp_th = est['hp_th_est']
        tes = est['tesla_est']

    # Feb 2026 extrapolation
    if m == '2026-02':
        # PGE CSV goes to Jan 2026 only. Feb 2026 will be handled separately.
        pass

    print(f"{m:<10} {peak_imp:>9.1f} {offpeak_imp:>9.1f} {peak_exp:>9.1f} {offpeak_exp:>9.1f} "
          f"{new_peak_credits:>9.1f} {new_offpeak_credits:>9.1f} {total_nm_used:>9.1f} "
          f"{cost_peak:>10.0f} {cost_offpeak:>10.0f} {cost_variable:>10.0f} {cost_fixed:>7.0f} {cost_total:>10.0f}{marker}")

    if in_analysis:
        results.append({
            'month': m, 'label': labels[m],
            'peak_import': peak_imp, 'offpeak_import': offpeak_imp,
            'peak_export': peak_exp, 'offpeak_export': offpeak_exp,
            'total_import': total_imp, 'total_export': total_exp,
            'peak_billed': peak_billed, 'offpeak_billed': offpeak_billed,
            'nm_used': total_nm_used,
            'cost_peak': cost_peak, 'cost_offpeak': cost_offpeak,
            'cost_variable': cost_variable, 'cost_fixed': cost_fixed,
            'cost_total': cost_total,
            'pv': pv, 'load': load, 'hp': hp, 'hp_th': hp_th, 'tesla': tes,
            'source': 'PGE',
            'offpeak_pct': offpeak_imp / total_imp * 100 if total_imp > 0 else 0,
        })

# Handle Feb 2026 (no PGE CSV data — use Zamel HA + estimated split)
# From PGE data, Feb 2025 had 70% offpeak. Using similar ratio.
# Zamel HA: import 2265.9, export 14.4 (to Feb 22, extrapolate to 28 days)
feb26_factor = 28 / 22
feb26_import = 2265.9 * feb26_factor
feb26_export = 14.4 * feb26_factor
feb26_offpeak_pct = 0.93  # Based on winter pattern (Nov-Jan were 93-97%)
feb26_peak_imp = feb26_import * (1 - feb26_offpeak_pct)
feb26_offpeak_imp = feb26_import * feb26_offpeak_pct
feb26_peak_exp = feb26_export * 0.3
feb26_offpeak_exp = feb26_export * 0.7

# NM credits for Feb 2026
new_peak_credits = feb26_peak_exp * NM_RATIO
new_offpeak_credits = feb26_offpeak_exp * NM_RATIO
nm_peak_bank += new_peak_credits
nm_offpeak_bank += new_offpeak_credits

peak_nm_used = min(nm_peak_bank, feb26_peak_imp)
nm_peak_bank -= peak_nm_used
feb26_peak_billed = feb26_peak_imp - peak_nm_used

offpeak_nm_available = nm_offpeak_bank + nm_peak_bank
offpeak_nm_used = min(offpeak_nm_available, feb26_offpeak_imp)
if offpeak_nm_used <= nm_offpeak_bank:
    nm_offpeak_bank -= offpeak_nm_used
else:
    excess_used = offpeak_nm_used - nm_offpeak_bank
    nm_offpeak_bank = 0
    nm_peak_bank -= excess_used
feb26_offpeak_billed = feb26_offpeak_imp - offpeak_nm_used

feb26_nm_used = peak_nm_used + offpeak_nm_used
feb26_cost_peak = feb26_peak_billed * PEAK_RATE
feb26_cost_offpeak = feb26_offpeak_billed * OFFPEAK_RATE
feb26_cost_variable = feb26_cost_peak + feb26_cost_offpeak
feb26_cost_total = feb26_cost_variable + FIXED_MONTHLY_TOTAL

# Feb 2026 load/HP/Tesla extrapolated
feb26_pv = fusion_solar_pv['2026-02'] * feb26_factor
feb26_load = total_load_ha['2026-02'] * feb26_factor
feb26_hp = hp_elec['2026-02'] * feb26_factor
feb26_hp_th = hp_thermal['2026-02'] * feb26_factor
feb26_tes = tesla_kwh['2026-02'] * feb26_factor

print(f"{'2026-02':<10} {feb26_peak_imp:>9.1f} {feb26_offpeak_imp:>9.1f} {feb26_peak_exp:>9.1f} {feb26_offpeak_exp:>9.1f} "
      f"{new_peak_credits:>9.1f} {new_offpeak_credits:>9.1f} {feb26_nm_used:>9.1f} "
      f"{feb26_cost_peak:>10.0f} {feb26_cost_offpeak:>10.0f} {feb26_cost_variable:>10.0f} {FIXED_MONTHLY_TOTAL:>7.0f} {feb26_cost_total:>10.0f} ← est")

results.append({
    'month': '2026-02', 'label': 'Lut 26*',
    'peak_import': feb26_peak_imp, 'offpeak_import': feb26_offpeak_imp,
    'peak_export': feb26_peak_exp, 'offpeak_export': feb26_offpeak_exp,
    'total_import': feb26_import, 'total_export': feb26_export,
    'peak_billed': feb26_peak_billed, 'offpeak_billed': feb26_offpeak_billed,
    'nm_used': feb26_nm_used,
    'cost_peak': feb26_cost_peak, 'cost_offpeak': feb26_cost_offpeak,
    'cost_variable': feb26_cost_variable, 'cost_fixed': FIXED_MONTHLY_TOTAL,
    'cost_total': feb26_cost_total,
    'pv': feb26_pv, 'load': feb26_load, 'hp': feb26_hp, 'hp_th': feb26_hp_th, 'tesla': feb26_tes,
    'source': 'HA*',
    'offpeak_pct': 93.0,
})

# ============================================================
# PART 6: Annual summary table
# ============================================================
print(f"\n\n{'='*180}")
print(f"  TABELA MIESIĘCZNA — ANALIZA (Mar 2025 - Feb 2026)")
print(f"{'='*180}")
print(f"\n{'Miesiąc':<10} {'Imp pk':>8} {'Imp off':>8} {'IMPORT':>8} {'Exp pk':>8} {'Exp off':>8} {'EKSPORT':>8} "
      f"{'NM użyte':>9} {'Rach pk':>8} {'Rach off':>8} {'Zmienne':>8} {'Stałe':>7} {'TOTAL':>8} "
      f"{'PV':>7} {'HP el':>7} {'Tesla':>7} {'%off':>6} {'Źródło':>7}")
print("-" * 180)

for r in results:
    print(f"{r['label']:<10} {r['peak_import']:>8.0f} {r['offpeak_import']:>8.0f} {r['total_import']:>8.0f} "
          f"{r['peak_export']:>8.0f} {r['offpeak_export']:>8.0f} {r['total_export']:>8.0f} "
          f"{r['nm_used']:>9.0f} {r['cost_peak']:>8.0f} {r['cost_offpeak']:>8.0f} "
          f"{r['cost_variable']:>8.0f} {r['cost_fixed']:>7.0f} {r['cost_total']:>8.0f} "
          f"{r['pv']:>7.0f} {r['hp']:>7.0f} {r['tesla']:>7.0f} {r['offpeak_pct']:>5.0f}% {r['source']:>7}")

# Totals
tot = {}
sum_keys = ['peak_import', 'offpeak_import', 'total_import', 'peak_export', 'offpeak_export',
            'total_export', 'nm_used', 'cost_peak', 'cost_offpeak', 'cost_variable',
            'cost_fixed', 'cost_total', 'pv', 'load', 'hp', 'hp_th', 'tesla',
            'peak_billed', 'offpeak_billed']
for k in sum_keys:
    tot[k] = sum(r[k] for r in results)

avg_offpeak_pct = tot['offpeak_import'] / tot['total_import'] * 100 if tot['total_import'] > 0 else 0

print("-" * 180)
print(f"{'ROCZNE':<10} {tot['peak_import']:>8.0f} {tot['offpeak_import']:>8.0f} {tot['total_import']:>8.0f} "
      f"{tot['peak_export']:>8.0f} {tot['offpeak_export']:>8.0f} {tot['total_export']:>8.0f} "
      f"{tot['nm_used']:>9.0f} {tot['cost_peak']:>8.0f} {tot['cost_offpeak']:>8.0f} "
      f"{tot['cost_variable']:>8.0f} {tot['cost_fixed']:>7.0f} {tot['cost_total']:>8.0f} "
      f"{tot['pv']:>7.0f} {tot['hp']:>7.0f} {tot['tesla']:>7.0f} {avg_offpeak_pct:>5.0f}%")

# ============================================================
# PART 7: Validation against PGE invoices
# ============================================================
print(f"\n\n{'='*100}")
print(f"  WALIDACJA vs FAKTURY PGE")
print(f"{'='*100}")

# Actual PGE invoice amounts (brutto)
pge_invoices = {
    '2025-08': {'brutto': 94.79, 'import': 103, 'billed_kwh': 0, 'note': 'All covered by NM'},
    '2025-10': {'brutto': 579.27, 'import': 1745, 'billed_kwh': 653, 'note': '653 offpeak billed'},
    '2025-11': {'brutto': 2626.67, 'import': 3005, 'billed_kwh': 3005, 'note': 'No NM credits'},
    '2025-12': {'brutto': 2846.67, 'import': 3408, 'billed_kwh': 3408, 'note': 'No NM credits'},
}

for m, inv in pge_invoices.items():
    my_result = next((r for r in results if r['month'] == m), None)
    if my_result:
        my_total = my_result['cost_total']
        diff = my_total - inv['brutto']
        diff_pct = diff / inv['brutto'] * 100 if inv['brutto'] > 0 else 0
        my_import = my_result['total_import']
        imp_diff = my_import - inv['import']
        print(f"\n  {m}:")
        print(f"    PGE faktura:  {inv['brutto']:>10.2f} PLN brutto  (import {inv['import']} kWh, {inv['note']})")
        print(f"    Moja analiza: {my_total:>10.2f} PLN brutto  (import {my_import:.0f} kWh)")
        print(f"    Różnica:      {diff:>+10.2f} PLN ({diff_pct:>+.1f}%)  import diff: {imp_diff:>+.0f} kWh")

# ============================================================
# PART 8: Full annual summary
# ============================================================
print(f"\n\n{'='*100}")
print(f"  PODSUMOWANIE ROCZNE (v5 — PGE dane godzinowe)")
print(f"{'='*100}")

gas_cost = 200  # Gas standby
total_energy_cost = tot['cost_total'] + gas_cost
avg_rate_billed = tot['cost_variable'] / (tot['peak_billed'] + tot['offpeak_billed']) if (tot['peak_billed'] + tot['offpeak_billed']) > 0 else OFFPEAK_RATE
avg_rate_gross = tot['cost_variable'] / tot['total_import'] if tot['total_import'] > 0 else OFFPEAK_RATE

print(f"\n  KOSZTY ENERGII ELEKTRYCZNEJ:")
print(f"    Import z sieci (brutto):           {tot['total_import']:>10,.0f} kWh")
print(f"      → w szczycie (peak):             {tot['peak_import']:>10,.0f} kWh ({tot['peak_import']/tot['total_import']*100:.1f}%)")
print(f"      → poza szczytem (offpeak):       {tot['offpeak_import']:>10,.0f} kWh ({tot['offpeak_import']/tot['total_import']*100:.1f}%)")
print(f"    Eksport do sieci:                  {tot['total_export']:>10,.0f} kWh")
print(f"    NM odzyskane (80%):                {tot['nm_used']:>10,.0f} kWh")
print(f"    Import do rozliczenia po NM:       {tot['peak_billed']+tot['offpeak_billed']:>10,.0f} kWh")
print(f"      → peak rozliczony:               {tot['peak_billed']:>10,.0f} kWh")
print(f"      → offpeak rozliczony:            {tot['offpeak_billed']:>10,.0f} kWh")
print(f"    Rachunek peak:                     {tot['cost_peak']:>10,.0f} PLN")
print(f"    Rachunek offpeak:                  {tot['cost_offpeak']:>10,.0f} PLN")
print(f"    Opłaty zmienne:                    {tot['cost_variable']:>10,.0f} PLN")
print(f"    Opłaty stałe ({FIXED_MONTHLY_TOTAL:.0f}×12):            {tot['cost_fixed']:>10,.0f} PLN")
print(f"    KOSZT PRĄDU ROCZNY:                {tot['cost_total']:>10,.0f} PLN")
print(f"    Gaz (szac. standby):               {gas_cost:>10} PLN")
print(f"    RAZEM ENERGIA ROCZNIE:             {total_energy_cost:>10,.0f} PLN ({total_energy_cost/12:>.0f} PLN/mies.)")
print(f"    Śr. cena rozliczonej kWh:          {avg_rate_billed:>10.3f} PLN/kWh brutto")

print(f"\n  PRODUKCJA PV (Fusion Solar):")
print(f"    Produkcja roczna:                  {tot['pv']:>10,.0f} kWh ({tot['pv']/1000:.2f} MWh)")
pv_self = tot['pv'] - tot['total_export']
pv_self_pct = pv_self / tot['pv'] * 100 if tot['pv'] > 0 else 0
print(f"    Autokonsumpcja:                    {pv_self:>10,.0f} kWh ({pv_self_pct:.0f}%)")
print(f"    Eksport do sieci:                  {tot['total_export']:>10,.0f} kWh ({100-pv_self_pct:.0f}%)")
nm_recovered = tot['nm_used']
print(f"    Odzyskane z NM 80%:                {nm_recovered:>10,.0f} kWh")
pv_effective = pv_self + nm_recovered
pv_eff_pct = pv_effective / tot['pv'] * 100 if tot['pv'] > 0 else 0
print(f"    Efektywne wykorzystanie PV:        {pv_effective:>10,.0f} kWh ({pv_eff_pct:.0f}%)")
strata = tot['total_export'] - nm_recovered
print(f"    Strata 20% NM:                     {strata:>10,.0f} kWh")

# PV savings: what would you pay without PV?
# Without PV: all load would be imported from grid
# With current offpeak ratio, cost would be:
cost_without_pv = tot['load'] * (avg_offpeak_pct/100 * OFFPEAK_RATE + (1-avg_offpeak_pct/100) * PEAK_RATE) + tot['cost_fixed'] + gas_cost
pv_savings_total = cost_without_pv - total_energy_cost
print(f"\n    Gdyby bez PV:                      {cost_without_pv:>10,.0f} PLN/rok")
print(f"    ŁĄCZNIE oszcz. PV:                 {pv_savings_total:>10,.0f} PLN")
pv_coverage = tot['pv'] / tot['load'] * 100 if tot['load'] > 0 else 0
print(f"    Pokrycie PV / zużycie:             {pv_coverage:>10.0f}%")

print(f"\n  POMPA CIEPŁA:")
print(f"    Elektryczność HP:                  {tot['hp']:>10,.0f} kWh")
print(f"    Ciepło wyprodukowane:              {tot['hp_th']:>10,.0f} kWh th")
cop = tot['hp_th'] / tot['hp'] if tot['hp'] > 0 else 3.0
print(f"    Średni COP roczny:                 {cop:>10.1f}")
hp_cost = tot['hp'] * OFFPEAK_RATE  # HP runs mostly at night (offpeak)
gas_eq = tot['hp_th'] * GAS_RATE
hp_savings = gas_eq - hp_cost
print(f"    Koszt HP (prąd, śr. offpeak):      {hp_cost:>10,.0f} PLN")
print(f"    Gdyby sam gaz:                     {gas_eq:>10,.0f} PLN")
print(f"    OSZCZĘDNOŚĆ HP vs GAZ:             {hp_savings:>10,.0f} PLN")

print(f"\n  TESLA EV:")
print(f"    Energia ładowania:                 {tot['tesla']:>10,.0f} kWh")
tesla_cost = tot['tesla'] * OFFPEAK_RATE  # Tesla charges at night (offpeak)
km_est = tot['tesla'] / 0.18
petrol_eq = km_est * 7 / 100 * 6.5
ev_savings = petrol_eq - tesla_cost
print(f"    Koszt ładowania (śr. offpeak):     {tesla_cost:>10,.0f} PLN")
print(f"    Gdyby spalinowy:                   {petrol_eq:>10,.0f} PLN")
print(f"    OSZCZĘDNOŚĆ EV vs SPALINOWY:       {ev_savings:>10,.0f} PLN")
print(f"    Szac. przebieg roczny:             {km_est:>10,.0f} km")

total_savings = pv_savings_total + hp_savings + ev_savings
print(f"\n  {'─'*60}")
print(f"  RAZEM OSZCZĘDNOŚCI:                  {total_savings:>10,.0f} PLN/rok")

print(f"\n\n  PORÓWNANIE wersji:")
print(f"    v4 (szac. stawki 0.72/0.43):  ~8 747 PLN/rok  (BŁĘDNE stawki!)")
print(f"    v5 (PGE godzinowe):           ~{total_energy_cost:,.0f} PLN/rok  (prawdziwe stawki + stałe)")
print(f"    Różnica: v5 jest o {total_energy_cost - 8747:+,.0f} PLN wyższy (stawki PGE ~70-83% wyższe!)")

# ============================================================
# PART 9: Key insights
# ============================================================
print(f"\n\n{'='*100}")
print(f"  KLUCZOWE WNIOSKI v5")
print(f"{'='*100}")
print(f"""
  1. STAWKI PGE są znacząco wyższe niż szacowane w v4:
     - Peak: {PEAK_RATE:.3f} vs 0.72 PLN/kWh (+{(PEAK_RATE/0.72-1)*100:.0f}%)
     - Offpeak: {OFFPEAK_RATE:.3f} vs 0.43 PLN/kWh (+{(OFFPEAK_RATE/0.43-1)*100:.0f}%)

  2. Import jest prawie w CAŁOŚCI offpeak ({avg_offpeak_pct:.0f}%):
     - HP i Tesla ładują nocą → offpeak
     - Deye pokrywa szczyt z baterii i PV → prawie zero importu peak
     - To OBNIŻA faktyczny koszt (offpeak jest 35% tańszy niż peak)

  3. NM działa per strefa z cross-zone transfer:
     - Latem eksport buduje zapas kredytów
     - Jesienią kredyty pokrywają import (Oct: 653 z 1745 kWh zbilansowane)
     - Od listopada kredyty wyczerpane → pełne rachunki

  4. Opłaty stałe to {tot['cost_fixed']:,.0f} PLN/rok ({tot['cost_fixed']/total_energy_cost*100:.0f}% kosztu):
     - Niezależne od zużycia
     - Handlowa (49.90) + mocowa (19.69) = 70 PLN/mies to główne składniki

  5. Bateria Deye (85 kWh) skutecznie eliminuje import peak:
     - Ładuje w nocy (offpeak) i weekendy
     - Rozładowuje w dzień → prawie zero importu w szczycie
     - Oszczędność: ~{tot['peak_import'] * (PEAK_RATE - OFFPEAK_RATE):,.0f} PLN/rok (gdyby nie bateria)
""")

# ============================================================
# EXPORT JSON for HTML
# ============================================================
print(f"\n\n--- DATA FOR HTML ---")
html_data = {
    'months': [r['label'] for r in results],
    'import': [round(r['total_import']) for r in results],
    'export': [round(r['total_export']) for r in results],
    'peak_import': [round(r['peak_import']) for r in results],
    'offpeak_import': [round(r['offpeak_import']) for r in results],
    'pv': [round(r['pv']) for r in results],
    'load': [round(r['load']) for r in results],
    'hp': [round(r['hp']) for r in results],
    'tesla': [round(r['tesla']) for r in results],
    'cost_total': [round(r['cost_total']) for r in results],
    'cost_variable': [round(r['cost_variable']) for r in results],
    'cost_fixed': [round(r['cost_fixed']) for r in results],
    'nm_used': [round(r['nm_used']) for r in results],
    'sources': [r['source'] for r in results],
    'offpeak_pct': [round(r['offpeak_pct'], 1) for r in results],
    'totals': {
        'cost_total': round(tot['cost_total']),
        'cost_variable': round(tot['cost_variable']),
        'cost_fixed': round(tot['cost_fixed']),
        'cost_with_gas': round(total_energy_cost),
        'monthly_avg': round(total_energy_cost/12),
        'import': round(tot['total_import']),
        'export': round(tot['total_export']),
        'peak_import': round(tot['peak_import']),
        'offpeak_import': round(tot['offpeak_import']),
        'nm_recovered': round(nm_recovered),
        'peak_billed': round(tot['peak_billed']),
        'offpeak_billed': round(tot['offpeak_billed']),
        'pv': round(tot['pv']),
        'pv_self': round(pv_self),
        'pv_self_pct': round(pv_self_pct),
        'pv_effective': round(pv_effective),
        'pv_eff_pct': round(pv_eff_pct),
        'pv_savings_total': round(pv_savings_total),
        'pv_coverage': round(pv_coverage),
        'cost_without_pv': round(cost_without_pv),
        'load': round(tot['load']),
        'hp': round(tot['hp']),
        'hp_thermal': round(tot['hp_th']),
        'cop': round(cop, 1),
        'hp_cost': round(hp_cost),
        'gas_eq': round(gas_eq),
        'hp_savings': round(hp_savings),
        'tesla': round(tot['tesla']),
        'tesla_cost': round(tesla_cost),
        'petrol_eq': round(petrol_eq),
        'ev_savings': round(ev_savings),
        'total_savings': round(total_savings),
        'km_est': round(km_est),
        'avg_rate': round(avg_rate_billed, 4),
        'peak_rate': round(PEAK_RATE, 4),
        'offpeak_rate': round(OFFPEAK_RATE, 4),
        'fixed_monthly': round(FIXED_MONTHLY_TOTAL, 2),
        'avg_offpeak_pct': round(avg_offpeak_pct, 1),
    }
}
print(json.dumps(html_data, indent=2))
