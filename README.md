Industry Scenario

Salon & Spa – Service Utilization & Customer Insights

Use Case

Analyze customer booking data to identify popular services, peak times, and improve customer experience.

Sample Dataset (CSV or DB)

Size: 50–100 records

customer_id, customer_name, gender, age

service_id, service_name, category (hair, skin, spa, grooming)

booking_id, booking_date, booking_time, amount_spent, duration, staff_id, status (completed/cancelled)

PySpark Core

Compute total revenue per service category.

Calculate average booking duration by service type.

PySpark SQL

Identify most popular services (highest number of bookings).

Find peak hours & peak days (group by booking_time/booking_date).

Show the above metrics in a DataFrame
