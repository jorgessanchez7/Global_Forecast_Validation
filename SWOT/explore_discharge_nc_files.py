import itertools
import pandas as pd
from netCDF4 import Dataset

# Archivos SWOT/SWORD
files = {
    "af": r"E:\SWOT\Discharge\af_sword_v16_SOS_results_unconstrained_20230502T204408_20250502T204408_20251219T163700.nc",
    "as": r"E:\SWOT\Discharge\as_sword_v16_SOS_results_unconstrained_20230502T204408_20250502T204408_20251219T163700.nc",
    "eu": r"E:\SWOT\Discharge\eu_sword_v16_SOS_results_unconstrained_20230502T204408_20250502T204408_20251219T163700.nc",
    "na": r"E:\SWOT\Discharge\na_sword_v16_SOS_results_unconstrained_20230502T204408_20250502T204408_20251219T163700.nc",
    "oc": r"E:\SWOT\Discharge\oc_sword_v16_SOS_results_unconstrained_20230502T204408_20250502T204408_20251219T163700.nc",
    "sa": r"E:\SWOT\Discharge\sa_sword_v16_SOS_results_unconstrained_20230502T204408_20250502T204408_20251219T163700.nc",
}

# Leer reach_id de cada archivo
reach_sets = {}

for region, path in files.items():
    print(f"Reading {region}: {path}")

    with Dataset(path) as ds:
        reach_ids = ds["reaches"]["reach_id"][:]

    # Convertir a enteros normales de Python para evitar problemas con numpy types
    reach_sets[region] = set(map(int, reach_ids))

    print(f"  {region}: {len(reach_sets[region]):,} reach_ids")


# Resumen por archivo
summary = pd.DataFrame({
    "region": list(reach_sets.keys()),
    "n_reach_ids": [len(v) for v in reach_sets.values()]
})

print("\n=== Reach IDs por archivo ===")
print(summary)


# Comparar traslapes por pares
overlap_records = []

for r1, r2 in itertools.combinations(reach_sets.keys(), 2):
    overlap = reach_sets[r1].intersection(reach_sets[r2])

    overlap_records.append({
        "region_1": r1,
        "region_2": r2,
        "n_overlap": len(overlap),
        "example_reach_ids": list(sorted(overlap))[:10]
    })

overlap_df = pd.DataFrame(overlap_records)

print("\n=== Traslapes por pares ===")
print(overlap_df)


# Guardar resultados
overlap_df.to_csv(
    r"E:\SWOT\Discharge\swot_reach_id_pairwise_overlaps.csv",
    index=False
)

summary.to_csv(
    r"E:\SWOT\Discharge\swot_reach_id_counts_by_region.csv",
    index=False
)