from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

DATE_FORMAT = "%m/%d/%Y"


class Sale:
    def __init__(
        self,
        region,
        country,
        item_type,
        sales_channel,
        order_priority,
        order_date,
        order_id,
        ship_date,
        units_sold,
        unit_price,
        unit_cost,
        total_revenue,
        total_cost,
        total_profit
    ):
        self.region = region
        self.country = country
        self.item_type = item_type
        self.sales_channel = sales_channel
        self.order_priority = self.format_priority(order_priority)

        self.order_date = order_date
        self.order_id = order_id
        self.ship_date = ship_date
        self.units_sold = units_sold

        self.unit_price = unit_price
        self.unit_cost = unit_cost
        self.total_revenue = total_revenue
        self.total_cost = total_cost
        self.total_profit = total_profit

        if self.order_date and self.ship_date:
            self.order_processing_time = (self.ship_date - self.order_date).days
        else:
            self.order_processing_time = 0

        self.gross_margin = (
            self.total_profit / self.total_revenue
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    def format_priority(self, priority):
        return {
            "L": "Low",
            "M": "Medium",
            "H": "High",
            "C": "Critical"
        }.get(priority, "")

    def __eq__(self, other):
        return isinstance(other, Sale) and self.order_id == other.order_id

    def __hash__(self):
        return hash(self.order_id)

    def __str__(self):
        return ",".join([
            self.region,
            self.country,
            self.item_type,
            self.sales_channel,
            self.order_priority,
            self.order_date.strftime(DATE_FORMAT),
            str(self.order_id),
            self.ship_date.strftime(DATE_FORMAT),
            str(self.units_sold),
            str(self.unit_price),
            str(self.unit_cost),
            str(self.total_revenue),
            str(self.total_cost),
            str(self.total_profit),
            str(self.order_processing_time),
            str(self.gross_margin)
        ])