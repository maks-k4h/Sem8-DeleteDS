#include <iostream>

#include "catalog.hpp"

#include <memory>





// Helper function to create and populate the coffee catalog
Database populate_coffee_database() {
    SystemCatalog catalog;

    // ================== SCHEMA DEFINITIONS ==================
    
    // Company Schema
    TableSchema company_schema{
        "Company",
        {
            {"C#", "string"},    // Company ID
            {"Name", "string"},  // Company name
            {"HQ", "string"}     // Headquarters city
        }
    };

    // Cafeteria Schema
    TableSchema cafeteria_schema{
        "Cafeteria",
        {
            {"CF#", "string"},    // Cafeteria ID
            {"Location", "string"},
            {"Seats", "int"}
        }
    };

    // Worker Schema
    TableSchema worker_schema{
        "Worker",
        {
            {"W#", "string"},    // Worker ID
            {"Name", "string"},
            {"Position", "string"}
        }
    };

    // City Schema
    TableSchema city_schema{
        "City",
        {
            {"CT#", "string"},   // City ID
            {"Name", "string"},
            {"Country", "string"}
        }
    };

    // ================== ADD SCHEMAS TO CATALOG ==================
    
    catalog.add_schema(company_schema);
    catalog.add_schema(cafeteria_schema);
    catalog.add_schema(worker_schema);
    catalog.add_schema(city_schema);

    // ================== DEFINE RELATIONSHIPS (FIXED TABLE NAMES) ==================
    // Corrected table names to match schemas
    RelationshipMetadata company_cafeterias{
        "CompanyCafeterias",
        {  // Owner (Company)
            "Company",  // Changed from "Companies"
            {  // Links
                {{"C1"}, {{"CF1"}, {"CF2"}}},
                {{"C2"}, {{"CF3"}}}
            }
        },
        {  // Member (Cafeteria)
            "Cafeteria",  // Changed from "Cafeterias"
            {  // Record groups
                {{"CF1"}}, {{"CF2"}}, {{"CF3"}}
            }
        }
    };

    RelationshipMetadata company_workers{
        "CompanyWorkers",
        {  // Owner
            "Company",  // Changed from "Companies"
            {
                {{"C1"}, {{"W1"}, {"W2"}}},
                {{"C2"}, {{"W3"}}}
            }
        },
        {  // Member
            "Worker",  // Changed from "Workers"
            {
                {{"W1"}}, {{"W2"}}, {{"W3"}}
            }
        }
    };

    RelationshipMetadata company_hq{
        "CompanyHQ",
        {
            "Company",  // Changed from "Companies"
            {
                {{"C1"}, {{"CT1"}}},
                {{"C2"}, {{"CT2"}}}
            }
        },
        {
            "City",  // Changed from "Cities"
            {
                {{"CT1"}}, {{"CT2"}}
            }
        }
    };

    // ================== ADD RELATIONSHIPS ==================
    catalog.add_relationship(company_cafeterias);
    catalog.add_relationship(company_workers);
    catalog.add_relationship(company_hq);

    Database db(catalog);

    // ================== POPULATE ALL TABLES ==================
    // Company Table
    DataTable company_table;
    TableRecord starbucks, blue_bottle;
    starbucks.set_field("C#", "SBUX");
    starbucks.set_field("Name", "Starbucks");
    starbucks.set_field("HQ", "Seattle");
    blue_bottle.set_field("C#", "BLUE");
    blue_bottle.set_field("Name", "Blue Bottle");
    blue_bottle.set_field("HQ", "Oakland");
    company_table.insert_record(starbucks);
    company_table.insert_record(blue_bottle);
    db.set_table("Company", company_table);

    // Cafeteria Table
    DataTable cafeteria_table;
    TableRecord cf1, cf2, cf3;
    cf1.set_field("CF#", "CF1");
    cf1.set_field("Location", "CT1");
    cf1.set_field("Seats", "50");
    cf2.set_field("CF#", "CF2");
    cf2.set_field("Location", "CT2");
    cf2.set_field("Seats", "40");
    cf3.set_field("CF#", "CF3");
    cf3.set_field("Location", "CT2");
    cf3.set_field("Seats", "30");
    cafeteria_table.insert_record(cf1);
    cafeteria_table.insert_record(cf2);
    cafeteria_table.insert_record(cf3);
    db.set_table("Cafeteria", cafeteria_table);

    // Worker Table
    DataTable worker_table;
    TableRecord w1, w2, w3;
    w1.set_field("W#", "W1");
    w1.set_field("Name", "Alice");
    w1.set_field("Position", "Manager");
    w2.set_field("W#", "W2");
    w2.set_field("Name", "Bob");
    w2.set_field("Position", "Barista");
    w3.set_field("W#", "W3");
    w3.set_field("Name", "Charlie");
    w3.set_field("Position", "Cashier");
    worker_table.insert_record(w1);
    worker_table.insert_record(w2);
    worker_table.insert_record(w3);
    db.set_table("Worker", worker_table);

    // City Table
    DataTable city_table;
    TableRecord ct1, ct2;
    ct1.set_field("CT#", "CT1");
    ct1.set_field("Name", "Seattle");
    ct1.set_field("Country", "USA");
    ct2.set_field("CT#", "CT2");
    ct2.set_field("Name", "Oakland");
    ct2.set_field("Country", "USA");
    city_table.insert_record(ct1);
    city_table.insert_record(ct2);
    db.set_table("City", city_table);

    return db;
}


int main() {

    auto db = populate_coffee_database();
    db.print();

    db.save_to_file("coffee.db");

    std::cout << "\nTesting serialization!!!\n\n";
    auto db1 = Database::load_from_file("coffee.db");
    db1.print();
    db1.deleteDS("CompanyCafeterias");
    db1.save_to_file("coffee1.db");

    return 0;
}
