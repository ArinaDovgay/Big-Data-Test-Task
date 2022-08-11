TAXI_DATA_PATH = "data/input/taxi_tripdata.csv"
RESULT_PATH = "data/output/task_result.csv"
NA = "N/A"


class Columns:
    vendor = "Vendor"
    vendor_id = "VendorID"
    payment_type = "Payment Type"
    payment_rate = "Payment Rate"
    next_payment_rate = "Next Payment Rate"
    max_payment_rate = "Max Payment Rate"
    percents_to_next_payment_rate = "Percents to next Rate"
    total_amount = "total_amount"
    passenger_count = "passenger_count"
    ta_per_vendor = "total_amount_per_vendor"
    pc_per_vendor = "passenger_count_per_vendor"
    payment_type_initial = "payment_type"


class VendorTypes:
    creative_mob_tech = "Creative Mobile Technologies, LLC"
    verifone = "VeriFone Inc"


class PaymentTypes:
    credit_card = "Credit card"
    cash = "Cash"
    no_charge = "No charge"
    dispute = "Dispute"
    unknown = "Unknown"
    voided_trip = "Voided trip"


columns = Columns
