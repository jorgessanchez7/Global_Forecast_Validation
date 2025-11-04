# ===============================================
# CDF del KGE para tres simulaciones GEOGloWS v1
# ===============================================
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

#region = 'south_america-geoglows'
region = 'north_america-geoglows'

# -----------------------------
# ARCHIVOS DE ENTRADA
# -----------------------------
path_v1      = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Error_Metrics\{region}\Metrics_GEOGloWS_v1_Q.csv"
path_rbc     = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Error_Metrics\{region}\Metrics_GEOGloWS_v1_RBC_Q.csv"
path_mfdcqm  = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Error_Metrics\{region}\Metrics_GEOGloWS_v1_MFDC-QM_Q.csv"

# -----------------------------
# LECTURA DE DATOS
# -----------------------------
df_v1     = pd.read_csv(path_v1)
df_rbc    = pd.read_csv(path_rbc)
df_mfdcqm = pd.read_csv(path_mfdcqm)

# El campo de interés
kge_v1     = df_v1["KGE"].dropna().values
kge_rbc    = df_rbc["KGE"].dropna().values
kge_mfdcqm = df_mfdcqm["KGE"].dropna().values

# -----------------------------
# FUNCIÓN PARA CDF
# -----------------------------
def compute_cdf(data):
    x = np.sort(data)
    y = np.arange(1, len(x)+1) / len(x)
    return x, y

x1, y1 = compute_cdf(kge_v1)
x2, y2 = compute_cdf(kge_rbc)
x3, y3 = compute_cdf(kge_mfdcqm)

# -----------------------------
# GRÁFICO
# -----------------------------
plt.figure(figsize=(10,6))

plt.plot(x1, y1, color='royalblue',  lw=2.0, label=f'GEOGloWS v1 (n={len(kge_v1)})')
plt.plot(x2, y2, color='darkorange', lw=2.0, label=f'Bias-Corrected Runoff (n={len(kge_rbc)})')
plt.plot(x3, y3, color='seagreen',   lw=2.0, label=f'MFDC-QM Bias Correction (n={len(kge_mfdcqm)})')

# Línea de referencia en KGE óptimo (=1)
plt.axvline(x=1.0, color='purple', lw=3, label='Optimum KGE')

# -----------------------------
# FORMATO
# -----------------------------
plt.xlabel('KGE', fontsize=12)
plt.ylabel('CDF', fontsize=12)
plt.title('Cumulative Distribution Function (CDF) of KGE metric for GEOGloWS v1 simulations', fontsize=13, weight='bold')
plt.legend(loc='upper left', fontsize=10)
plt.grid(True, linestyle='--', linewidth=0.8, alpha=0.6)

# Activar subgrilla y hacerla más densa
plt.minorticks_on()
plt.grid(which='minor', linestyle=':', linewidth=0.5, alpha=0.5)

# Espaciado de ticks principales
plt.xticks(np.arange(-1, 1.01, 0.2))
plt.yticks(np.arange(0, 1.01, 0.1))

plt.xlim(-1, 1)
plt.ylim(0, 1)

# -----------------------------
# GUARDAR FIGURA
# -----------------------------
plt.tight_layout()
plt.savefig(r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Error_Metrics\{region}\CDF_KGE_GEOGLOWS_v1.png", dpi=400)
plt.show()
