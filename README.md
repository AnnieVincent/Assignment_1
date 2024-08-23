# Youtube data harvesting and warehousing

YouTube Data Harvesting and Warehousing Introduction YouTube Data Harvesting and Warehousing is a project aimed at developing a user-friendly Streamlit application that leverages the power of the Google API to extract valuable information from YouTube channels. The extracted data is then stored in a MongoDB database, subsequently migrated to a SQL data warehouse, and made accessible for analysis and exploration within the Streamlit app.

Storing Data in MongoDB The retrieved data is stored in a MongoDB database based on user authorization. If the data already exists in the database, it can be overwritten with user consent. This storage process ensures efficient data management and preservation, allowing for seamless handling of the collected data.

Migrating Data to a SQL Data Warehouse The application allows users to migrate data from MongoDB to a SQL data warehouse. Users can choose which channel's data to migrate. To ensure compatibility with a structured format, the data is cleansed using the powerful pandas library. Following data cleaning, the information is segregated into separate tables, including channels, playlists, videos, and comments, utilizing SQL queries.

Data Analysis The project provides comprehensive data analysis capabilities using Streamlit. The Streamlit app offers an intuitive interface to interact with the data, allowing users to customize visualizations, filter data, and explore insights. Although Plotly was initially mentioned, it is not used in the current version of the project.
