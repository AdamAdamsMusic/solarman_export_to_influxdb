from datetime import datetime
import pandas as pd
import json

# Define a mapping from column names to units
units = {
    'sofar_solar_8_8ktlx_g3_napiecie_pradu_pv1': 'V',
    'sofar_solar_8_8ktlx_g3_napiecie_pradu_pv2': 'V',
    'sofar_solar_8_8ktlx_g3_natezenie_pradu_pv1': 'A',
    'sofar_solar_8_8ktlx_g3_natezenie_pradu_pv2': 'A',
    'sofar_solar_8_8ktlx_g3_moc_pv1': 'W',
    'sofar_solar_8_8ktlx_g3_moc_pv2': 'W',
    'sofar_solar_8_8ktlx_g3_faza_r_napiecie': 'V',
    'sofar_solar_8_8ktlx_g3_faza_s_napiecie': 'V',
    'sofar_solar_8_8ktlx_g3_faza_t_napiecie': 'V',
    'sofar_solar_8_8ktlx_g3_faza_r_natezenie': 'A',
    'sofar_solar_8_8ktlx_g3_faza_s_natezenie': 'A',
    'sofar_solar_8_8ktlx_g3_faza_t_natezenie': 'A',
    'sofar_solar_8_8ktlx_g3_czestotliwosc_sieci': 'Hz',
    'sofar_solar_8_8ktlx_g3_calkowita_moc_czynna_na_wyjsciu': 'W',
    'sofar_solar_8_8ktlx_g3_wygenerowana_energia_ogolem': 'kWh',
    'sofar_solar_8_8ktlx_g3_wygenerowana_energia_dzis': 'kWh',
    'sofar_solar_8_8ktlx_g3_calkowita_moc_bierna_na_wyjsciu': 'W',
    'sofar_solar_8_8ktlx_g3_opornosc_izolacji': 'kOhm',
    'sofar_solar_8_8ktlx_g3_temperatura_modulu_1': '°C',
    'sofar_solar_8_8ktlx_g3_temperatura_chlodnicy_1': '°C',
    'sofar_solar_8_8ktlx_g3_czas_wytwarzania_dzis': 'minutes',
    'sofar_solar_8_8ktlx_g3_czas_wytwarzania_ogolem': 'minutes',
    'sofar_solar_8_8ktlx_g3_inverter_status': 'state',
}

# Define a mapping from units to device classes
device_classes = {
    'V': 'voltage',
    'A': 'current',
    'W': 'power',
    '°C': 'temperature',
    'kWh': 'energy',
    'minutes': 'none',
    'kohm': 'none',
    'Hz': 'current'
}

# Mapping from column names to friendly names in the Home Assistant/InfluxDB
friendly_names = {
    'sofar_solar_8_8ktlx_g3_napiecie_pradu_pv1': 'SOFAR Solar 8.8KTLX-G3 Napięcie prądu PV1',
    'sofar_solar_8_8ktlx_g3_napiecie_pradu_pv2': 'SOFAR Solar 8.8KTLX-G3 Napięcie prądu PV2',
    'sofar_solar_8_8ktlx_g3_natezenie_pradu_pv1': 'SOFAR Solar 8.8KTLX-G3 Natężenie prądu PV1',
    'sofar_solar_8_8ktlx_g3_natezenie_pradu_pv2': 'SOFAR Solar 8.8KTLX-G3 Natężenie prądu PV2',
    'sofar_solar_8_8ktlx_g3_moc_pv1': 'SOFAR Solar 8.8KTLX-G3 Moc PV1',
    'sofar_solar_8_8ktlx_g3_moc_pv2': 'SOFAR Solar 8.8KTLX-G3 Moc PV2',
    'sofar_solar_8_8ktlx_g3_faza_r_napiecie': 'SOFAR Solar 8.8KTLX-G3 Faza R - Napięcie',
    'sofar_solar_8_8ktlx_g3_faza_s_napiecie': 'SOFAR Solar 8.8KTLX-G3 Faza S - Napięcie',
    'sofar_solar_8_8ktlx_g3_faza_t_napiecie': 'SOFAR Solar 8.8KTLX-G3 Faza T - Napięcie',
    'sofar_solar_8_8ktlx_g3_faza_r_natezenie': 'SOFAR Solar 8.8KTLX-G3 Faza R - Natężenie',
    'sofar_solar_8_8ktlx_g3_faza_s_natezenie': 'SOFAR Solar 8.8KTLX-G3 Faza S - Natężenie',
    'sofar_solar_8_8ktlx_g3_faza_t_natezenie': 'SOFAR Solar 8.8KTLX-G3 Faza T - Natężenie',
    'sofar_solar_8_8ktlx_g3_czestotliwosc_sieci': 'SOFAR Solar 8.8KTLX-G3 Częstotliwość sieci',
    'sofar_solar_8_8ktlx_g3_calkowita_moc_czynna_na_wyjsciu': 'SOFAR Solar 8.8KTLX-G3 Całkowita moc czynna na wyściu',
    'sofar_solar_8_8ktlx_g3_wygenerowana_energia_ogolem': 'SOFAR Solar 8.8KTLX-G3 Wygenerowana energia - ogółem',
    'sofar_solar_8_8ktlx_g3_wygenerowana_energia_dzis': 'SOFAR Solar 8.8KTLX-G3 Wygenerowana energia - dziś',
    'sofar_solar_8_8ktlx_g3_calkowita_moc_bierna_na_wyjsciu': 'SOFAR Solar 8.8KTLX-G3 Całkowita moc bierna na wyściu',
    'sofar_solar_8_8ktlx_g3_temperatura_modulu_1': 'SOFAR Solar 8.8KTLX-G3 Temperatura modułu 1',
    'sofar_solar_8_8ktlx_g3_temperatura_chlodnicy_1': 'SOFAR Solar 8.8KTLX-G3 Temperatura chłodnicy 1',
    'sofar_solar_8_8ktlx_g3_czas_wytwarzania_dzis': 'SOFAR Solar 8.8KTLX-G3 Czas wytwarzania - dziś',
    'sofar_solar_8_8ktlx_g3_czas_wytwarzania_ogolem': 'SOFAR Solar 8.8KTLX-G3 Czas wytwarzania - ogólem',
    'sofar_solar_8_8ktlx_g3_inverter_status': 'SOFAR Solar 8.8KTLX-G3 Inverter status',
}

# Mapping for Home Assistant icons in InfluxDB entries, just for completion sake
icon_strs = {
    'sofar_solar_8_8ktlx_g3_napiecie_pradu_pv1': 'mdi:solar-power',
    'sofar_solar_8_8ktlx_g3_napiecie_pradu_pv2': 'mdi:solar-power',
    'sofar_solar_8_8ktlx_g3_natezenie_pradu_pv1': 'mdi:solar-power',
    'sofar_solar_8_8ktlx_g3_natezenie_pradu_pv2': 'mdi:solar-power',
    'sofar_solar_8_8ktlx_g3_moc_pv1': 'mdi:solar-power',
    'sofar_solar_8_8ktlx_g3_moc_pv2': 'mdi:solar-power',
    'sofar_solar_8_8ktlx_g3_faza_r_napiecie': 'mdi:solar-power',
    'sofar_solar_8_8ktlx_g3_faza_s_napiecie': 'mdi:solar-power',
    'sofar_solar_8_8ktlx_g3_faza_t_napiecie': 'mdi:solar-power',
    'sofar_solar_8_8ktlx_g3_faza_r_natezenie': 'mdi:lightning-bolt-outline',
    'sofar_solar_8_8ktlx_g3_faza_s_natezenie': 'mdi:lightning-bolt-outline',
    'sofar_solar_8_8ktlx_g3_faza_t_natezenie': 'mdi:lightning-bolt-outline',
    'sofar_solar_8_8ktlx_g3_czestotliwosc_sieci': 'mdi:home-lightning-bolt',
    'sofar_solar_8_8ktlx_g3_calkowita_moc_czynna_na_wyjsciu': 'mdi:home-lightning-bolt',
    'sofar_solar_8_8ktlx_g3_wygenerowana_energia_ogolem': 'mdi:solar-power',
    'sofar_solar_8_8ktlx_g3_wygenerowana_energia_dzis': 'mdi:solar-power',
    'sofar_solar_8_8ktlx_g3_calkowita_moc_bierna_na_wyjsciu': 'mdi:home-lightning-bolt',
    'sofar_solar_8_8ktlx_g3_temperatura_modulu_1': 'mdi:thermometer',
    'sofar_solar_8_8ktlx_g3_temperatura_chlodnicy_1': 'mdi:thermometer',
    'sofar_solar_8_8ktlx_g3_czas_wytwarzania_dzis': 'mdi:clock',
    'sofar_solar_8_8ktlx_g3_czas_wytwarzania_ogolem': 'mdi:clock',
    'sofar_solar_8_8ktlx_g3_inverter_status': 'mdi:wrench',
}

# Specify the date range
start_date = datetime(2023, 3, 14, 23)  # Start date (year, month, day)
end_date = datetime(2023, 3, 17, 2)  # End date (year, month, day)

start_timestamp = int(start_date.timestamp())
end_timestamp = int(end_date.timestamp())

df = pd.read_excel('data.xlsx')  # Read Excel file

with open('output.txt', 'w', encoding='utf-8') as out:
    for index, row in df.iterrows():
        timestamp = int(pd.to_datetime(row['timestamp'], format='%Y/%m/%d %H:%M').timestamp())  # Convert timestamp to seconds

        # Only process rows where timestamp is within the desired range
        if start_timestamp <= timestamp <= end_timestamp:
            for key, value in row.items():
                if key != 'timestamp' and pd.notnull(value):
                    unit = units.get(key, '')
                    friendly_name_str = json.dumps(friendly_names.get(key, ''), ensure_ascii=False) # Enclose in double quotes
                    icon_str = json.dumps(icon_strs.get(key, ''), ensure_ascii=False)
                    device_class = json.dumps(device_classes.get(unit, ''), ensure_ascii=False)

                    # Check if the key is 'inverter_status'
                    if key == 'sofar_solar_8_8ktlx_g3_inverter_status':
                        field_name = 'state'
                        value = json.dumps(value, ensure_ascii=False)
                    else:
                        field_name = 'value'
                        value = float(value)    # Convert to float as it is InfluxDB default

                    line = f'{unit},domain=sensor,entity_id={key} device_class_str={device_class},friendly_name=8.83,friendly_name_str={friendly_name_str},icon_str={icon_str},state_class_str="measurement",{field_name}={value} {timestamp}\n'
                    out.write(line)