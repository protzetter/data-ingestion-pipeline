"""
Integration tests for the Electra charging stations pipeline.
"""
import os
import pytest
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Import the functions from the pipeline script
# Note: This assumes the run_electra_pipeline.py is properly set up as a module
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from run_electra_pipeline import (
    get_spark_session,
    get_schema,
    read_electra_csv,
    validate_data,
    transform_data
)


@pytest.fixture
def spark():
    """Create a Spark session for testing."""
    return get_spark_session(app_name="TestElectraPipeline", master="local[1]")


@pytest.fixture
def sample_electra_data(spark, tmp_path):
    """Create a sample CSV file with Electra charging station data."""
    csv_path = tmp_path / "sample_electra.csv"
    
    with open(csv_path, "w") as f:
        f.write("id_pdc_itinerance,observations,prise_type_2,prise_type_autre,prise_type_chademo,prise_type_combo_ccs,prise_type_ef,puissance_nominale,nom_amenageur,siren_amenageur,contact_amenageur,nom_operateur,contact_operateur,nom_enseigne,id_station_itinerance,nom_station,implantation_station,adresse_station,coordonneesXY,nbre_pdc,gratuit,paiement_acte,paiement_cb,paiement_autre,condition_acces,reservation,horaires,accessibilite_pmr,restriction_gabarit,station_deux_roues,raccordement,num_pdl,date_mise_en_service,date_maj\n")
        f.write("FRELCEDSRN,Test,false,false,false,true,false,150,ELECTRA,891624884,help@electra.com,ELECTRA,help@electra.com,ELECTRA,FRELCPVIRHC,Viriat - Hôtel,Station dédiée,Parc d'activités 01440 Viriat,[5.20522700,46.22200400],4,false,true,true,true,Accès libre,true,24/7,Accessibilité inconnue,Inconnu,false,Direct,N/A,2023-01-15,2023-01-16\n")
        f.write("FRELCEJB39,Test,true,false,false,false,false,50,ELECTRA,891624884,help@electra.com,ELECTRA,help@electra.com,ELECTRA,FRELCPVIRHC,Paris - Centre,Station dédiée,Avenue de Paris 75001 Paris,[2.35222700,48.85200400],2,true,false,true,true,Accès libre,false,24/7,PMR,Inconnu,true,Direct,N/A,2023-02-20,2023-02-21\n")
        f.write("FRELCEHQVU,Test,false,false,true,false,false,100,ELECTRA,891624884,help@electra.com,ELECTRA,help@electra.com,ELECTRA,FRELCPVIRHC,Lyon - Gare,Station dédiée,Place de la gare 69000 Lyon,[4.85222700,45.75200400],6,false,true,true,false,Accès libre,true,24/7,PMR,Inconnu,false,Direct,N/A,invalid-date,2023-03-15\n")
    
    return str(csv_path)


def test_read_electra_csv(spark, sample_electra_data):
    """Test reading Electra CSV file."""
    df = read_electra_csv(spark, sample_electra_data)
    
    # Check that data was loaded correctly
    assert df.count() == 3
    assert len(df.columns) == 34
    
    # Check specific values
    first_row = df.filter(F.col("id_pdc_itinerance") == "FRELCEDSRN").collect()[0]
    assert first_row["puissance_nominale"] == 150
    assert first_row["nom_station"] == "Viriat - Hôtel"


def test_validate_data(spark, sample_electra_data):
    """Test data validation for Electra data."""
    df = read_electra_csv(spark, sample_electra_data)
    validation_results = validate_data(df)
    
    # Check validation results
    assert "null_counts" in validation_results
    assert "duplicate_ids" in validation_results
    assert "invalid_dates" in validation_results
    
    # We expect one invalid date in our sample data
    assert validation_results["invalid_dates"] == 1


def test_transform_data(spark, sample_electra_data):
    """Test data transformation for Electra data."""
    df = read_electra_csv(spark, sample_electra_data)
    transformed_df = transform_data(df)
    
    # Check that transformation was applied correctly
    assert "longitude" in transformed_df.columns
    assert "latitude" in transformed_df.columns
    assert "power_category" in transformed_df.columns
    assert "charging_type" in transformed_df.columns
    assert "processed_timestamp" in transformed_df.columns
    
    # Check specific transformations
    high_power = transformed_df.filter(F.col("power_category") == "High").count()
    medium_power = transformed_df.filter(F.col("power_category") == "Medium").count()
    low_power = transformed_df.filter(F.col("power_category") == "Low").count()
    
    assert high_power == 1  # One station with power >= 150
    assert medium_power == 1  # One station with 50 <= power < 150
    assert low_power == 1  # One station with power < 50
    
    # Check charging types
    ccs_count = transformed_df.filter(F.col("charging_type") == "CCS").count()
    type2_count = transformed_df.filter(F.col("charging_type") == "Type 2").count()
    chademo_count = transformed_df.filter(F.col("charging_type") == "CHAdeMO").count()
    
    assert ccs_count == 1
    assert type2_count == 1
    assert chademo_count == 1


def test_end_to_end_pipeline(spark, sample_electra_data, tmp_path):
    """Test the entire pipeline from reading to transformation."""
    # Read data
    df = read_electra_csv(spark, sample_electra_data)
    
    # Validate data
    validation_results = validate_data(df)
    
    # Transform data
    transformed_df = transform_data(df)
    
    # Save data to a temporary location
    output_path = str(tmp_path / "test_output")
    transformed_df.write.format("parquet").mode("overwrite").save(output_path)
    
    # Read back the saved data to verify
    saved_df = spark.read.parquet(output_path)
    
    # Verify the saved data
    assert saved_df.count() == 3
    assert "longitude" in saved_df.columns
    assert "latitude" in saved_df.columns
    assert "power_category" in saved_df.columns