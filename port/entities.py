import re


def build_cost_entity(report_data, blueprint):
    for identifier, lines in report_data.items():
        entity = {"identifier": re.compile('[^A-Za-z0-9@_.:/=-]*').sub('', identifier),
                  "blueprint": blueprint,
                  "properties": {
                      "unblendedCost": 0,
                      "blendedCost": 0,
                      "amortizedCost": 0,
                      "ondemandCost": 0,
                      "payingAccount": lines[0]["bill/PayerAccountId"],
                      "usageAccount": lines[0]["lineItem/UsageAccountId"],
                      "billStartDate": lines[0]["bill/BillingPeriodStartDate"],
                      "product": lines[0]["product/ProductName"],
                      "resourceId": lines[0]["lineItem/ResourceId"],
                      "operation":lines[0]["lineItem/Operation"],
                      "kubernetesServiceName":lines[0]["resourceTags/user:kubernetes.io/service-name"],
                      "environment":lines[0]["resourceTags/user:environment"]
                      }
                  }
        for line in lines:
            entity["properties"]["unblendedCost"] += float(line.get("lineItem/NetUnblendedCost")
                                                            or line.get("lineItem/UnblendedCost") or 0)
            entity["properties"]["blendedCost"] += float(line.get("lineItem/BlendedCost", 0))
            entity["properties"]["amortizedCost"] += _calc_amortized_cost(line)
            entity["properties"]["ondemandCost"] += float(line.get("pricing/publicOnDemandCost", 0))
        yield entity


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
