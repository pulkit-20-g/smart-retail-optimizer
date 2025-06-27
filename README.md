# Retail Insights Dashboard

A comprehensive, data-driven inventory and sales analysis tool for multi-store retail operations, built using **SQL** and **Tableau**.

##  Project Overview

This project addresses critical retail challenges like stockouts, overstocking, and misaligned inventory strategies. Through structured SQL analytics and dynamic Tableau dashboards, it empowers decision-makers to visualize and act on key insights across stores and products.

## 🔍 Key Features

- 📊 **Interactive Dashboards** to monitor product movement, turnover, and understocking.
- 📈 **Seasonal and Promotional Analysis** to uncover demand patterns.
- ⚙️ **SQL Scripts** for generating reorder reports, sales impact studies, and product-level metrics.
- 🌦️ **Weather & External Factor Mapping** to correlate external conditions with sales behavior.

 🛠️ Tech Stack

- **SQL (PostgreSQL)** – For all backend querying and data modeling.
- **Tableau** – For visualization and interactive dashboards.
- *Python – For preprocessing or automation (if required).

## Folder Structure

```
├
│── inventory_queries.sql
│── sales_analysis_queries.sql
├
│── dashboards.twbx
├
│── inventory_forcasting.csv
├── README.md
```

## Use Cases

- Inventory planning and optimization
- Product classification and movement tracking
- Regional sales strategy refinement
- Promotion effectiveness evaluation

##  Insights You Can Derive

- Top understocked SKUs by store
- Movement category vs Days of Holding (DOH) mapping
- Fastest and slowest inventory turnover items
- Store-specific performance comparisons

## Sample Visuals

> <img width="1472" alt="Screenshot 2025-06-26 at 2 01 55 AM" src="https://github.com/user-attachments/assets/3158d967-d64b-48d9-b3a9-9bf1ca3d0c96" />


## How to Use

1. Load the sample data into your SQL database.
2. Run provided SQL scripts to extract and transform insights.
3. Connect Tableau to your DB or `.csv` exports.
4. Use built dashboards or modify them as needed.

## Future Extensions

- Real-time alert integration
- Predictive stocking using machine learning
- API endpoints for live data sync

---

**Project by Pulkit Garg & Lakshita Agarwalla, IIT Guwahati**  
_Transforming retail decisions through data._
