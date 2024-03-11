import re


def build_cost_entity(report_data, blueprint):
    for identifier, lines in report_data.items():
        entity = {"identifier": re.compile('[^A-Za-z0-9@_.:/=-]*').sub('', identifier),
                  "blueprint": blueprint,
                  "properties": {"unblendedCost": 0, "blendedCost": 0, "amortizedCost": 0, "ondemandCost": 0,
                                 "payingAccount": lines[0]["bill/PayerAccountId"],
                                 "usageAccount": lines[0]["lineItem/UsageAccountId"],
                                 "billStartDate": lines[0]["bill/BillingPeriodStartDate"],
                                 "product": lines[0]["product/ProductName"]}}
        for line in lines:
            entity["properties"]["unblendedCost"] += float(line.get("lineItem/NetUnblendedCost")
                                                            or line.get("lineItem/UnblendedCost") or 0)
            entity["properties"]["blendedCost"] += float(line.get("lineItem/BlendedCost", 0))
            entity["properties"]["amortizedCost"] += _calc_amortized_cost(line)
            entity["properties"]["ondemandCost"] += float(line.get("pricing/publicOnDemandCost", 0))
        yield entity

def build_cloud_resource_entity(data, blueprint):
    for identifier, lines in data.items():
        arn = (
            identifier.split("@")[0][len("arn:") :]
            if identifier.startswith("arn:")
            else None
        )

        if not arn:
            continue

        title = extract_and_capitalize_resource_name(arn)
        # build cloud resource entity
        cloud_resource = {
            "identifier": lines[0]["lineItem/ResourceId"],
            "title": title,
            "blueprint": blueprint,
            "properties": {"service": lines[0]["lineItem/ProductCode"], "region": lines[0]["product/region"] },
            "relations": {"cloud-account": lines[0]["lineItem/UsageAccountId"]},
        }
        yield cloud_resource


def extract_and_capitalize_resource_name(arn):
    """Extracts the resource name from an ARN and capitalizes it."""
    parts = arn.split(":")

    if parts:  # Basic check for ARN structure
        resource_name = parts[-1]
        return resource_name
    else:
        return None  # Handle invalid ARN

def _calc_amortized_cost(line):
    # Based on: https://wellarchitectedlabs.com/cost-optimization/cur_queries/queries/global/
    line_item_type = line.get("lineItem/LineItemType")
    if line_item_type == "SavingsPlanCoveredUsage":
        return float(line.get("savingsPlan/NetSavingsPlanEffectiveCost") or
                     line.get("savingsPlan/SavingsPlanEffectiveCost") or 0)
    elif line_item_type == "SavingsPlanRecurringFee":
        return float(line.get("savingsPlan/TotalCommitmentToDate")) - float(line.get("savingsPlan/UsedCommitment"))
    elif line_item_type == "SavingsPlanNegation":
        return 0
    elif line_item_type == "SavingsPlanUpfrontFee":
        return 0
    elif line_item_type == "DiscountedUsage":
        return float(line.get("reservation/NetEffectiveCost") or line.get("reservation/EffectiveCost") or 0)
    elif line_item_type == "RIFee":
        return (float(line.get("reservation/NetUnusedAmortizedUpfrontFeeForBillingPeriod") or
                      line.get("reservation/UnusedAmortizedUpfrontFeeForBillingPeriod") or 0) +
                float(line.get("reservation/NetUnusedRecurringFee") or line.get("reservation/UnusedRecurringFee") or 0))
    elif line_item_type == "Fee" and line.get("reservation/ReservationARN"):
        return 0
    else:
        return float(line.get("lineItem/NetUnblendedCost") or line.get("lineItem/UnblendedCost") or 0)
