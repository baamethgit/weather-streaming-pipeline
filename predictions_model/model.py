import numpy as np

def predict_temperature_4h(temperature, humidity, wind_speed, pressure=None):
    """
    Prédit les températures des 4 prochaines heures
    
    Args:
        temperature: list des températures des dernières heures
        humidity: list de l'humidité relative (%)
        wind_speed: list de la vitesse du vent (km/h)
        pressure: list pression atmosphérique (optionnel)
    
    Returns:
        dict: {'H+1': temp, 'H+2': temp, 'H+3': temp, 'H+4': temp}
    """
    
    if len(temperature) < 6:
        return {"error": "Minimum 6 heures de données nécessaires"}
    
    # Prendre les 6 dernières valeurs
    recent_temp = temperature[-6:]
    recent_humidity = humidity[-6:] if len(humidity) >= 6 else [50] * 6
    recent_wind = wind_speed[-6:] if len(wind_speed) >= 6 else [0] * 6
    
    predictions = {}
    
    for h in range(1, 5):  # H+1 à H+4
        
        # 1. Base : moyenne pondérée des températures
        weights = [0.4, 0.25, 0.15, 0.1, 0.05, 0.05]  # Plus récent = plus important
        base_temp = sum(t * w for t, w in zip(recent_temp, weights))
        
        # 2. Tendance température (régression simple)
        x = np.arange(6)
        temp_trend = np.polyfit(x, recent_temp, 1)[0]  # Pente
        
        # 3. Correction humidité
        # Humidité élevée (>80%) → refroidissement
        # Humidité faible (<30%) → réchauffement
        current_humidity = recent_humidity[-1]
        if current_humidity > 80:
            humidity_effect = -0.5
        elif current_humidity < 30:
            humidity_effect = 0.3
        else:
            humidity_effect = 0
        
        # 4. Correction vent
        # Vent fort → refroidissement par convection
        current_wind = recent_wind[-1]
        if current_wind > 20:
            wind_effect = -0.8
        elif current_wind > 10:
            wind_effect = -0.3
        else:
            wind_effect = 0
        
        # 5. Prédiction finale
        predicted_temp = (
            base_temp + 
            (temp_trend * h * 0.7) +  # Tendance diminue avec le temps
            humidity_effect + 
            wind_effect
        )
        
        predictions[f"H+{h}"] = round(predicted_temp, 1)
    
    return predictions


def get_prediction_confidence(temperature):
    """
    Calcule un score de confiance basé sur la stabilité des données
    """
    if len(temperature) < 6:
        return 0
    
    # Variance des 6 dernières heures
    variance = np.var(temperature[-6:])
    
    # Plus la variance est faible, plus on est confiant
    if variance < 1:
        return 0.9  # Très confiant
    elif variance < 4:
        return 0.7  # Confiant
    elif variance < 9:
        return 0.5  # Moyen
    else:
        return 0.3  # Peu confiant


# Exemple d'utilisation
if __name__ == "__main__":
    
    # Données des 8 dernières heures
    temp_data = [18.5, 19.2, 20.1, 21.3, 22.0, 21.8, 21.5, 21.2]
    humidity_data = [65, 68, 70, 72, 71, 69, 67, 66]
    wind_data = [5, 7, 8, 12, 15, 18, 16, 14]
    
    # Prédiction
    predictions = predict_temperature_4h(temp_data, humidity_data, wind_data)
    confidence = get_prediction_confidence(temp_data)
    
    print("Prédictions:")
    for hour, temp in predictions.items():
        print(f"  {hour}: {temp}°C")
    
    print(f"\nConfiance: {confidence*100:.0f}%")