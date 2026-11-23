
import csv
from pathlib import Path
from collections import Counter

# Chemin du CSV original, à la racine du projet
ROOT = Path(__file__).resolve().parents[2]
CSV_FILE = ROOT / "data" / "bronze" / "meteorite_landings.csv"


def to_float(value):
    """Convertit une valeur en nombre, ou retourne None."""
    if value is None or value.strip() == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def main():
    if not CSV_FILE.exists():
        raise FileNotFoundError(f"Fichier introuvable : {CSV_FILE}")

    with CSV_FILE.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    total = len(rows)

    missing = Counter()
    fall_types = Counter()
    ids = Counter()

    zero_mass = 0
    negative_mass = 0
    invalid_latitude = 0
    longitude_0_360 = 0
    invalid_longitude = 0
    missing_any_coordinate = 0
    invalid_year = 0

    for row in rows:
        # Valeurs manquantes
        for field in ["mass (g)", "year", "reclat", "reclong"]:
            if not row[field].strip():
                missing[field] += 1

        # Identifiants et catégories
        ids[row["id"]] += 1
        fall_types[row["fall"]] += 1

        # Masse
        mass = to_float(row["mass (g)"])
        if mass is not None:
            if mass == 0:
                zero_mass += 1
            elif mass < 0:
                negative_mass += 1

        # Année
        year = to_float(row["year"])
        if year is not None and (not year.is_integer() or year <= 0):
            invalid_year += 1

        # Coordonnées
        lat = to_float(row["reclat"])
        lon = to_float(row["reclong"])

        if lat is None or lon is None:
            missing_any_coordinate += 1

        if lat is not None and not -90 <= lat <= 90:
            invalid_latitude += 1

        if lon is not None:
            if 180 < lon <= 360:
                longitude_0_360 += 1
            elif not -180 <= lon <= 180:
                invalid_longitude += 1

    duplicate_ids = sum(count - 1 for count in ids.values() if count > 1)

    print("\n===== METEORITE HUNTER - DATA QUALITY =====")
    print(f"Fichier : {CSV_FILE.name}")
    print(f"Nombre de lignes : {total}")

    print("\n--- Répartition ---")
    for category, count in fall_types.items():
        print(f"{category}: {count}")

    print("\n--- Valeurs manquantes ---")
    for field, count in missing.items():
        print(f"{field}: {count}")

    print("\n--- Anomalies ---")
    print(f"Masses égales à zéro : {zero_mass}")
    print(f"Masses négatives : {negative_mass}")
    print(f"Années invalides : {invalid_year}")
    print(f"Latitudes invalides : {invalid_latitude}")
    print(f"Longitudes au format 0-360 : {longitude_0_360}")
    print(f"Longitudes invalides : {invalid_longitude}")
    print(f"Lignes avec au moins une coordonnée manquante : {missing_any_coordinate}")
    print(f"Identifiants dupliqués : {duplicate_ids}")

    print("\n===== FIN DU CONTROLE =====")


if __name__ == "__main__":
    main()
