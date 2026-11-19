"""
build_ruleset.py - write the versioned category ruleset.

The original pipeline excluded 162 Google categories by *falling through a
lookup* rather than by any decision, and split Costco from Sam's Club because
Google said "Warehouse store" for one and "Warehouse club" for the other. The
cure is that every category string a source can emit gets an explicit row here,
with a tier, an inclusion flag and a written rationale. A category the frame
build has never seen is a hard error, not a silent drop.

Tiers
  CORE       full-line food retail: the anchor stores of food access
  FRESH      fresh/perishable outlets: produce, meat, seafood, markets
  SPECIALTY  ethnic, health and specialty grocery
  MARGINAL   sells food but the store is not primarily a food retailer
  OUT        not a food retailer

include is (tier != OUT). Widening to MARGINAL is deliberate: exclusion should
be an analysis filter, not data the pipeline destroyed. food_access_type is the
tag an analyst filters on.
"""
import csv
from collections import Counter
from pathlib import Path

OUT = Path(__file__).parent / "category_ruleset.csv"

# (overture_category, tier, food_access_type, rationale)
RULES = [
    # ---- CORE -------------------------------------------------------------
    ("supermarket", "CORE", "supermarket", "Full-line grocery; the primary food-access anchor."),
    ("superstore", "CORE", "supercenter", "Walmart/Target-class full-line grocery."),
    ("grocery_store", "CORE", "grocery", "Full-line grocery."),
    ("convenience_store", "CORE", "convenience", "Dominant food source in low-access areas; central to the research question."),
    ("discount_store", "CORE", "dollar", "Dollar General/Family Dollar/Dollar Tree. MISSED BY THE 2026-01 PULL as a primary category."),
    ("department_store", "CORE", "supercenter", "Walmart/Target carry full grocery. MISSED BY THE 2026-01 PULL as a primary category."),
    ("wholesale_store", "CORE", "warehouse_club", "Costco/Sams/BJs. The Warehouse store vs Warehouse club split that lost all 9 NC Costcos."),
    ("warehouses", "CORE", "warehouse_club", "Same vocabulary problem; adjudicate per store on the chain name."),
    ("food", "CORE", "grocery", "Generic Overture food-retail bucket; resolve per store downstream."),

    # ---- FRESH ------------------------------------------------------------
    ("farmers_market", "FRESH", "farmers_market", "Fresh produce access point; excluded by the old filter and lost twice."),
    ("public_market", "FRESH", "farmers_market", "Public/municipal market hall."),
    ("night_market", "FRESH", "farmers_market", "Periodic market."),
    ("holiday_market", "FRESH", "farmers_market", "Periodic market."),
    ("flea_market", "FRESH", "farmers_market", "Many NC flea markets carry substantial produce; tag and let the analyst decide."),
    ("fruits_and_vegetables", "FRESH", "produce", "Produce stand/greengrocer."),
    ("butcher_shop", "FRESH", "butcher", "Fresh meat outlet."),
    ("meat_wholesaler", "FRESH", "butcher", "Some sell retail; adjudicate per store."),
    ("fishmonger", "FRESH", "seafood", "Fresh seafood outlet."),
    ("seafood_market", "FRESH", "seafood", "Fresh seafood outlet; coastal NC relevance."),
    ("seafood_wholesaler", "FRESH", "seafood", "Some sell retail; adjudicate per store."),
    ("produce_wholesaler", "FRESH", "produce", "Some sell retail; adjudicate per store."),
    ("dairy_stores", "FRESH", "specialty", "Retail dairy."),
    ("csa_farm", "FRESH", "farm_direct", "Community-supported agriculture; direct food access."),
    ("honey_farm_shop", "FRESH", "farm_direct", "On-farm retail."),
    ("urban_farm", "FRESH", "farm_direct", "Often runs a stand; adjudicate per store."),
    ("agricultural_cooperatives", "FRESH", "farm_direct", "Some operate retail outlets; adjudicate per store."),

    # ---- SPECIALTY --------------------------------------------------------
    ("health_food_store", "SPECIALTY", "health_food", "Natural/health grocery."),
    ("health_market", "SPECIALTY", "health_food", "Natural/health grocery."),
    ("organic_grocery_store", "SPECIALTY", "health_food", "Organic grocery."),
    ("ethical_grocery", "SPECIALTY", "health_food", "Co-op/ethical grocery."),
    ("international_grocery_store", "SPECIALTY", "ethnic_grocery", "Ethnic grocery; serves populations mainstream chains underserve."),
    ("asian_grocery_store", "SPECIALTY", "ethnic_grocery", "Ethnic grocery."),
    ("indian_grocery_store", "SPECIALTY", "ethnic_grocery", "Ethnic grocery."),
    ("korean_grocery_store", "SPECIALTY", "ethnic_grocery", "Ethnic grocery."),
    ("japanese_grocery_store", "SPECIALTY", "ethnic_grocery", "Ethnic grocery."),
    ("mexican_grocery_store", "SPECIALTY", "ethnic_grocery", "Ethnic grocery; large NC Hispanic population."),
    ("russian_grocery_store", "SPECIALTY", "ethnic_grocery", "Ethnic grocery."),
    ("kosher_grocery_store", "SPECIALTY", "ethnic_grocery", "Ethnic/religious grocery."),
    ("imported_food", "SPECIALTY", "ethnic_grocery", "Import grocery."),
    ("specialty_grocery_store", "SPECIALTY", "specialty", "Specialty grocery."),
    ("specialty_foods", "SPECIALTY", "specialty", "Specialty food retail."),
    ("delicatessen", "SPECIALTY", "specialty", "Deli counter retail; distinguish from restaurants downstream."),
    ("cheese_shop", "SPECIALTY", "specialty", "Specialty food retail."),
    ("herb_and_spice_shop", "SPECIALTY", "specialty", "Specialty food retail."),
    ("frozen_foods", "SPECIALTY", "specialty", "Retail frozen food."),
    ("wholesale_grocer", "SPECIALTY", "specialty", "Some sell retail; adjudicate per store."),

    # ---- MARGINAL ---------------------------------------------------------
    ("gas_station", "MARGINAL", "gas_convenience", "NC gas stations very often ARE the convenience store. Tagged, not dropped."),
    ("farm", "MARGINAL", "farm_direct", "Only some run a farm stand; needs per-store adjudication."),
    ("poultry_farm", "MARGINAL", "farm_direct", "Only some sell retail."),
    ("dairy_farm", "MARGINAL", "farm_direct", "Only some sell retail."),
    ("fish_farm", "MARGINAL", "farm_direct", "Only some sell retail."),
    ("attraction_farm", "MARGINAL", "farm_direct", "Agritourism; some sell produce."),
    ("food_banks", "MARGINAL", "food_assistance", "Not retail, but a real food-access channel. Tagged for the analyst."),
    ("vitamins_and_supplements", "MARGINAL", "health_food", "Mostly supplements, not food. Tagged low priority."),
    ("bakery", "MARGINAL", "specialty", "Prepared food; only sometimes a staple source."),
    ("food_court", "MARGINAL", "prepared", "Prepared food."),
    ("pharmacy", "MARGINAL", "gas_convenience", "CVS/Walgreens carry shelf-stable groceries in low-access areas."),
    ("shopping", "MARGINAL", "unknown", "Generic Overture bucket; resolve per store."),

    # ---- OUT --------------------------------------------------------------
    ("restaurant", "OUT", "", "Prepared food service, not retail."),
    ("fast_food_restaurant", "OUT", "", "Prepared food service."),
    ("seafood_restaurant", "OUT", "", "Prepared food service."),
    ("mexican_restaurant", "OUT", "", "Prepared food service."),
    ("pizza_restaurant", "OUT", "", "Prepared food service."),
    ("comfort_food_restaurant", "OUT", "", "Prepared food service."),
    ("soul_food", "OUT", "", "Prepared food service."),
    ("health_food_restaurant", "OUT", "", "Prepared food service."),
    ("halal_restaurant", "OUT", "", "Prepared food service."),
    ("fish_and_chips_restaurant", "OUT", "", "Prepared food service."),
    ("cheesesteak_restaurant", "OUT", "", "Prepared food service."),
    ("live_and_raw_food_restaurant", "OUT", "", "Prepared food service."),
    ("restaurant_wholesale", "OUT", "", "Trade supply, not consumer retail."),
    ("food_truck", "OUT", "", "Mobile prepared food; no fixed address to scrape."),
    ("coffee_shop", "OUT", "", "Prepared beverage."),
    ("food_delivery_service", "OUT", "", "Service, not a store."),
    ("pizza_delivery_service", "OUT", "", "Service, not a store."),
    ("courier_and_delivery_services", "OUT", "", "Logistics."),
    ("water_delivery", "OUT", "", "Service, not a store."),
    ("food_beverage_service_distribution", "OUT", "", "Trade distribution."),
    ("b2b_food_products", "OUT", "", "Trade distribution."),
    ("food_and_beverage_exporter", "OUT", "", "Trade."),
    ("importer_and_exporter", "OUT", "", "Trade."),
    ("importers", "OUT", "", "Trade."),
    ("wholesaler", "OUT", "", "Generic trade wholesale."),
    ("computer_wholesaler", "OUT", "", "Not food."),
    ("furniture_wholesalers", "OUT", "", "Not food."),
    ("fabric_wholesaler", "OUT", "", "Not food."),
    ("optical_wholesaler", "OUT", "", "Not food."),
    ("fitness_equipment_wholesaler", "OUT", "", "Not food."),
    ("pharmaceutical_products_wholesaler", "OUT", "", "Not food."),
    ("nutritionist", "OUT", "", "Professional service."),
    ("food_consultant", "OUT", "", "Professional service."),
    ("food_tours", "OUT", "", "Tourism."),
    ("farming_services", "OUT", "", "Agricultural service."),
    ("farm_equipment_and_supply", "OUT", "", "Equipment."),
    ("farming_equipment_store", "OUT", "", "Equipment."),
    ("farm_equipment_repair_service", "OUT", "", "Equipment service."),
    ("hunting_and_fishing_supplies", "OUT", "", "Sporting goods."),
    ("fishing_charter", "OUT", "", "Recreation."),
    ("fishing_club", "OUT", "", "Recreation."),
    ("fire_department", "OUT", "", "Keyword false positive (contains department)."),
    ("police_department", "OUT", "", "Keyword false positive."),
    ("health_department", "OUT", "", "Keyword false positive."),
    ("department_of_motor_vehicles", "OUT", "", "Keyword false positive."),
    ("department_of_social_service", "OUT", "", "Keyword false positive."),
    ("marketing_agency", "OUT", "", "Keyword false positive (contains market)."),
    ("marketing_consultant", "OUT", "", "Keyword false positive."),
    ("internet_marketing_service", "OUT", "", "Keyword false positive."),
    ("telemarketing_services", "OUT", "", "Keyword false positive."),
    ("hospice", "OUT", "", "Keyword false positive."),
    ("pet_hospice", "OUT", "", "Keyword false positive."),
    ("altering_and_remodeling_contractor", "OUT", "", "Keyword false positive."),
    ("bathroom_remodeling", "OUT", "", "Keyword false positive."),
    ("kitchen_remodeling", "OUT", "", "Keyword false positive."),
    ("closet_remodeling", "OUT", "", "Keyword false positive."),
    ("fidelity_and_surety_bonds", "OUT", "", "Keyword false positive."),
    # Strings that reach the pull only via categories.alternate. They are ruled
    # explicitly so that "no ruling" always means "nobody has looked at this",
    # never "the lookup happened to miss".
    ("rental_kiosks", "OUT", "", "Redbox-style kiosk, not a store."),
    ("flowers_and_gifts_shop", "OUT", "", "Not food retail."),
    ("sandwich_shop", "OUT", "", "Prepared food."),
    ("video_and_video_game_rentals", "OUT", "", "Not food."),
    ("clothing_store", "OUT", "", "Not food."),
    ("tobacco_shop", "OUT", "", "Not food."),
    ("liquor_store", "OUT", "", "Alcohol only; out of scope for food access."),
    ("beer_wine_and_spirits_store", "OUT", "", "Alcohol only."),
    ("thrift_store", "OUT", "", "Not food."),
    ("hardware_store", "OUT", "", "Not food."),
    ("pet_store", "OUT", "", "Not human food."),
    ("automotive_repair", "OUT", "", "Not food."),
    ("atms", "OUT", "", "Not a store."),
    ("bank_credit_union", "OUT", "", "Not a store."),
    ("check_cashing_service", "OUT", "", "Not a store."),
    ("money_transfer_services", "OUT", "", "Not a store."),
    ("mobile_phone_store", "OUT", "", "Not food."),
    ("laundromat", "OUT", "", "Not food."),
    ("car_wash", "OUT", "", "Not food."),
    ("hotel", "OUT", "", "Not food retail."),
    ("gym", "OUT", "", "Not food retail."),
    ("shopping_district", "OUT", "", "Not a store."),
    ("shopping_mall", "MARGINAL", "unknown", "Container; resolve per store."),
    ("discount_supermarket", "CORE", "supermarket", "Aldi/Lidl-class."),
    ("general_store", "CORE", "grocery", "Rural general store."),
    ("bodega", "CORE", "convenience", "Neighbourhood grocery."),
    ("variety_store", "CORE", "dollar", "Dollar-store class."),
]


# ---------------------------------------------------------------------------
# USDA SNAP store types. Seven values, assigned by a federal reviewer at
# authorisation, and the most trustworthy classification in the whole frame.
# "Other" is genuinely mixed and resolves against the other vocabularies.
# ---------------------------------------------------------------------------
SNAP_RULES = [
    ("Supermarket", "CORE", "supermarket", "FNS classification: full-line grocery."),
    ("Super Store", "CORE", "supercenter", "FNS classification: supercenter."),
    ("Grocery Store", "CORE", "grocery", "FNS classification: grocery."),
    ("Convenience Store", "CORE", "convenience", "FNS classification: convenience."),
    ("Farmers and Markets", "FRESH", "farmers_market", "FNS classification: market/direct producer."),
    ("Specialty Store", "SPECIALTY", "specialty", "FNS classification: specialty food retail."),
    ("Other", "MARGINAL", "unknown", "FNS catch-all; resolve against Overture/OSM/Google in the same cluster."),
]

# ---------------------------------------------------------------------------
# OpenStreetMap shop/amenity values.
# ---------------------------------------------------------------------------
OSM_RULES = [
    ("supermarket", "CORE", "supermarket", "OSM shop=supermarket."),
    ("convenience", "CORE", "convenience", "OSM shop=convenience."),
    ("grocery", "CORE", "grocery", "OSM shop=grocery."),
    ("general", "CORE", "grocery", "OSM shop=general: rural general store."),
    ("department_store", "CORE", "supercenter", "OSM shop=department_store."),
    ("variety_store", "CORE", "dollar", "OSM shop=variety_store: dollar stores."),
    ("wholesale", "CORE", "warehouse_club", "OSM shop=wholesale: warehouse clubs."),
    ("greengrocer", "FRESH", "produce", "OSM shop=greengrocer."),
    ("butcher", "FRESH", "butcher", "OSM shop=butcher."),
    ("seafood", "FRESH", "seafood", "OSM shop=seafood."),
    ("farm", "FRESH", "farm_direct", "OSM shop=farm: farm shop, retail by definition of the tag."),
    ("dairy", "FRESH", "specialty", "OSM shop=dairy."),
    ("marketplace", "FRESH", "farmers_market", "OSM amenity=marketplace."),
    ("health_food", "SPECIALTY", "health_food", "OSM shop=health_food."),
    ("deli", "SPECIALTY", "specialty", "OSM shop=deli."),
    ("cheese", "SPECIALTY", "specialty", "OSM shop=cheese."),
    ("spices", "SPECIALTY", "specialty", "OSM shop=spices."),
    ("frozen_food", "SPECIALTY", "specialty", "OSM shop=frozen_food."),
    ("food", "SPECIALTY", "specialty", "OSM shop=food."),
    ("bakery", "MARGINAL", "specialty", "OSM shop=bakery."),
    ("nutrition_supplements", "MARGINAL", "health_food", "OSM shop=nutrition_supplements."),
]

# ---------------------------------------------------------------------------
# Google Maps categories. 623 distinct strings appear across master and the
# exclusion audit; the top 178 cover 95% of rows. Google is a SECONDARY
# vocabulary here - a cluster is ruled by SNAP/Overture/OSM wherever those are
# present, because those are small controlled vocabularies that can be ruled
# exhaustively. These rulings decide clusters where Google is the only
# evidence. Anything unruled becomes an UNRULED cluster on a worklist; it is
# never silently dropped, which is the failure this whole file exists to stop.
#
# Note "Warehouse store" / "Warehouse club" / "Warehouse": the old sheet ruled
# these inconsistently, which is the entire reason no Costco reached the
# dataset while Sam's Club and BJ's did.
# ---------------------------------------------------------------------------
GOOGLE_RULES = [
    ("Supermarket", "CORE", "supermarket", ""),
    ("Grocery store", "CORE", "grocery", ""),
    ("Convenience store", "CORE", "convenience", ""),
    ("Convenience store, Gas station", "CORE", "convenience", "Primary category is the convenience store."),
    ("Dollar store", "CORE", "dollar", ""),
    ("Discount store", "CORE", "dollar", ""),
    ("Variety store", "CORE", "dollar", ""),
    ("Discount supermarket", "CORE", "supermarket", ""),
    ("Department store", "CORE", "supercenter", ""),
    ("General store", "CORE", "grocery", ""),
    ("Warehouse club", "CORE", "warehouse_club", "Sam's Club, BJ's."),
    ("Warehouse store", "CORE", "warehouse_club", "Costco. Ruled IN here; the old sheet ruled this synonym out."),
    ("Warehouse", "CORE", "warehouse_club", "Same synonym family; adjudicate per store on the chain name."),
    ("Store", "CORE", "grocery", "Generic Google label; resolve per store."),
    ("Market", "CORE", "grocery", "Generic Google label; resolve per store."),
    ("Food store", "CORE", "grocery", ""),
    ("Farmers' market", "FRESH", "farmers_market", ""),
    ("Flea market", "FRESH", "farmers_market", ""),
    ("Produce market", "FRESH", "produce", ""),
    ("Fruit and vegetable store", "FRESH", "produce", ""),
    ("Produce wholesaler", "FRESH", "produce", "Some sell retail."),
    ("Butcher shop", "FRESH", "butcher", ""),
    ("Butcher shop deli", "FRESH", "butcher", ""),
    ("Meat processor", "FRESH", "butcher", "Many run a retail counter."),
    ("Meat products store", "FRESH", "butcher", ""),
    ("Meat wholesaler", "FRESH", "butcher", "Some sell retail."),
    ("Seafood market", "FRESH", "seafood", ""),
    ("Seafood wholesaler", "FRESH", "seafood", "Some sell retail."),
    ("Fish store", "FRESH", "seafood", ""),
    ("Farm shop", "FRESH", "farm_direct", ""),
    ("Pick your own farm produce", "FRESH", "farm_direct", ""),
    ("Orchard", "FRESH", "farm_direct", ""),
    ("Honey farm", "FRESH", "farm_direct", ""),
    ("Organic farm", "FRESH", "farm_direct", ""),
    ("Dairy farm", "FRESH", "farm_direct", "Some sell retail."),
    ("Egg supplier", "FRESH", "farm_direct", ""),
    ("Health food store", "SPECIALTY", "health_food", ""),
    ("Natural goods store", "SPECIALTY", "health_food", ""),
    ("Gourmet grocery store", "SPECIALTY", "specialty", ""),
    ("Mexican grocery store", "SPECIALTY", "ethnic_grocery", ""),
    ("Asian grocery store", "SPECIALTY", "ethnic_grocery", ""),
    ("Indian grocery store", "SPECIALTY", "ethnic_grocery", ""),
    ("African goods store", "SPECIALTY", "ethnic_grocery", ""),
    ("Mexican goods store", "SPECIALTY", "ethnic_grocery", ""),
    ("Oriental goods store", "SPECIALTY", "ethnic_grocery", ""),
    ("International grocery store", "SPECIALTY", "ethnic_grocery", ""),
    ("Korean grocery store", "SPECIALTY", "ethnic_grocery", ""),
    ("Japanese grocery store", "SPECIALTY", "ethnic_grocery", ""),
    ("Chinese supermarket", "SPECIALTY", "ethnic_grocery", ""),
    ("Tienda de alimentacion", "SPECIALTY", "ethnic_grocery", "Spanish-language label Google emits for NC tiendas; unruled by the old sheet."),
    ("Eastern European grocery store", "SPECIALTY", "ethnic_grocery", "Unruled by the old sheet."),
    ("American grocery store", "SPECIALTY", "grocery", "Unruled by the old sheet."),
    ("Carniceria", "SPECIALTY", "ethnic_grocery", "Unruled by the old sheet."),
    ("Deli", "SPECIALTY", "specialty", ""),
    ("Delicatessen", "SPECIALTY", "specialty", ""),
    ("Herb shop", "SPECIALTY", "specialty", ""),
    ("Spice store", "SPECIALTY", "specialty", ""),
    ("Cheese shop", "SPECIALTY", "specialty", ""),
    ("Candy store", "SPECIALTY", "specialty", ""),
    ("Wine store", "SPECIALTY", "specialty", ""),
    ("Food products supplier", "SPECIALTY", "specialty", ""),
    ("Food producer", "SPECIALTY", "specialty", ""),
    ("Gas station", "MARGINAL", "gas_convenience", "In rural NC the gas station is frequently the only food retail."),
    ("Truck stop", "MARGINAL", "gas_convenience", ""),
    ("Drug store", "MARGINAL", "gas_convenience", "Carries shelf-stable groceries."),
    ("Pharmacy", "MARGINAL", "gas_convenience", "Carries shelf-stable groceries."),
    ("Farm", "MARGINAL", "farm_direct", "Only some run a stand."),
    ("Bakery", "MARGINAL", "specialty", ""),
    ("Vitamin & supplements store", "MARGINAL", "health_food", ""),
    ("Sports nutrition store", "MARGINAL", "health_food", ""),
    ("Animal feed store", "MARGINAL", "unknown", "Rural general merchants sometimes carry food."),
    ("Wholesaler", "MARGINAL", "unknown", "Resolve per store."),
    ("Distribution service", "MARGINAL", "unknown", "Resolve per store."),
    ("Shopping mall", "MARGINAL", "unknown", "Resolve per store."),
    ("Garden center", "MARGINAL", "farm_direct", "Some carry produce."),
    ("Plant nursery", "OUT", "", "Not food retail."),
    ("Christmas tree farm", "OUT", "", "Not food retail."),
    ("Liquor store", "OUT", "", "Alcohol only; out of scope for food access."),
    ("Tobacco shop", "OUT", "", "Not food."),
    ("Smoke shop", "OUT", "", "Not food."),
    ("Vaporizer store", "OUT", "", "Not food."),
    ("Cannabis store", "OUT", "", "Not food."),
    ("Restaurant", "OUT", "", "Prepared food."),
    ("restaurants", "OUT", "", "Prepared food."),
    ("Fast food restaurant", "OUT", "", "Prepared food."),
    ("Mexican restaurant", "OUT", "", "Prepared food."),
    ("Seafood restaurant", "OUT", "", "Prepared food."),
    ("cafes", "OUT", "", "Prepared food."),
    ("Ice cream shop", "OUT", "", "Prepared food."),
    ("Juice shop", "OUT", "", "Prepared food."),
    ("Donut shop", "OUT", "", "Prepared food."),
    ("Popcorn store", "OUT", "", "Confection only."),
    ("Coffee shop", "OUT", "", "Prepared beverage."),
    ("ATM", "OUT", "", "Not a store."),
    ("Bank", "OUT", "", "Not a store."),
    ("attractions", "OUT", "", "Not food retail."),
    ("Event venue", "OUT", "", "Not food retail."),
    ("Wedding venue", "OUT", "", "Not food retail."),
    ("Corporate office", "OUT", "", "Not a store."),
    ("Non-profit organization", "OUT", "", "Not retail."),
    ("Manufacturer", "OUT", "", "Not retail."),
    ("Food manufacturer", "OUT", "", "Not retail."),
    ("Agricultural production", "OUT", "", "Not retail."),
    ("Hardware store", "OUT", "", "Not food."),
    ("Furniture store", "OUT", "", "Not food."),
    ("Antique store", "OUT", "", "Not food."),
    ("Clothing store", "OUT", "", "Not food."),
    ("Gift shop", "OUT", "", "Not food."),
    ("Auto repair shop", "OUT", "", "Not food."),
    ("Nutritionist", "OUT", "", "Professional service."),
    ("Wellness center", "OUT", "", "Not food retail."),
    ("Bottled water supplier", "OUT", "", "Not food."),
    ("Ice supplier", "OUT", "", "Not food."),
    ("Fuel supplier", "OUT", "", "Fuel distribution, not a retail store."),
    ("Garden", "OUT", "", "Not food retail."),
    ("Beer store", "OUT", "", "Alcohol only."),
    ("Wholesale bakery", "OUT", "", "Trade supply."),
    ("Taco restaurant", "OUT", "", "Prepared food."),
    ("coffee shops", "OUT", "", "Prepared beverage."),
    ("Medical clinic", "OUT", "", "Not food retail."),
    ("Park", "OUT", "", "Not food retail."),
    ("Tienda de productos mexicanos", "SPECIALTY", "ethnic_grocery", "Spanish-language Google label for a Mexican goods store."),
    ("Supermercado", "CORE", "supermarket", "Spanish-language Google label."),
    ("Fruteria", "FRESH", "produce", "Spanish-language Google label for a produce store."),
    ("Panaderia", "MARGINAL", "specialty", "Spanish-language Google label for a bakery."),
]

VOCABS = [
    ("overture", RULES),
    ("snap", SNAP_RULES),
    ("osm", OSM_RULES),
    ("google", GOOGLE_RULES),
]


def main():
    rows = []
    for vocab, rules in VOCABS:
        seen = set()
        for cat, tier, fat, why in rules:
            assert cat not in seen, f"duplicate ruling for {vocab}/{cat}"
            seen.add(cat)
            rows.append({
                "vocabulary": vocab,
                "category": cat,
                "tier": tier,
                "include": tier != "OUT",
                "food_access_type": fat,
                "rationale": why,
            })

    with OUT.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    print(f"wrote {len(rows)} rulings -> {OUT}")
    for vocab, _ in VOCABS:
        sub = [r for r in rows if r["vocabulary"] == vocab]
        c = Counter(r["tier"] for r in sub)
        inc = sum(1 for r in sub if r["include"])
        print(f"  {vocab:9s} {len(sub):4d} rulings   "
              + "  ".join(f"{t}={c[t]}" for t in ("CORE", "FRESH", "SPECIALTY", "MARGINAL", "OUT"))
              + f"   included={inc}")


if __name__ == "__main__":
    main()
