# Personalized Outfit Recommendations for E-Commerce

The future of retail depends on offering products tailored to each customer’s preferences. The pandemic has driven a significant increase in online shopping, with brands like H&M seeing a surge in sales and shifting focus to their websites. To adapt, it’s crucial to develop e-commerce platforms that accurately capture customer preferences.

This project aims to recommend outfits to customers based on their body shape. Beyond standard filters like price, size, and color, our approach uses the H&M dataset to build a system where customers input details such as age, size, body shape, color preferences, skin tone, and fabric preferences to receive personalized outfit recommendations.

Using these parameters, the platform will display only those outfits that are best suited to the customer. Additionally, if the customer has any textile-related skin conditions, the system will suggest clothing made from appropriate fabrics. The project also considers other factors to provide useful insights into product selection.

## Data

The data used in this project is available in the GitHub repository under the `src/scripts` folder. Due to size limitations, some datasets are hosted externally and can be accessed via a Google Drive link shared with all SJSU accounts.

## Code

The primary focus of the scripts is to achieve the following objectives:

- **Body Shape Identification**: For a given set of customer measurements (hip, bust, waist, and high hip), the system identifies the body shape (rectangle, triangle, round, hourglass, inverted triangle).
- **Outfit Recommendation**: Based on the identified body shape and any skin conditions (1-severe, 2-mild, 3-no issues), the system recommends the best suitable outfit for the selected product type (e.g., top, trouser, skirt).

## Methodology

- **Data Processing**: Performed ETL (Extract, Transform, Load) using AWS Redshift, S3, and Glue to efficiently load, process, and prepare the data for analysis.
- **Data Analysis**: Utilized Redshift queries and Tableau visualizations to analyze customer demographics, spending patterns, inventory levels, and sales trends.

## Documentation

All related documents, reports, and screenshots are included in the repository. They provide further details on the project’s implementation and results.

---


