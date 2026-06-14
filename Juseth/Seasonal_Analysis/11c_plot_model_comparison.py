from PIL import Image
import numpy as np

# ============================================================
# INPUT — paths to the three generated figures
# ============================================================
PATH_BIAS        = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Figure_7a_bias_Maps.png"
PATH_VARIABILITY = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Figure_7b_variability_Maps.png"
PATH_CORRELATION = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Figure_7c_correlation_Maps.png"
OUTPUT_FIG       = r"E:\Post_Doc\GEOGLOWS_Applications\Runoff_Bias_Correction\GEOGLOWS_v1\Figure_7_KGE_components_combined.png"

# ============================================================
# LOAD IMAGES
# ============================================================
img_bias  = Image.open(PATH_BIAS)
img_var   = Image.open(PATH_VARIABILITY)
img_corr  = Image.open(PATH_CORRELATION)

# ============================================================
# ENSURE SAME HEIGHT (resize if needed)
# ============================================================
h = img_bias.height
if img_var.height != h:
    img_var = img_var.resize((int(img_var.width * h / img_var.height), h), Image.LANCZOS)
if img_corr.height != h:
    img_corr = img_corr.resize((int(img_corr.width * h / img_corr.height), h), Image.LANCZOS)

# ============================================================
# COMBINE HORIZONTALLY (side by side)
# ============================================================
total_width = img_bias.width + img_var.width + img_corr.width
combined = Image.new("RGB", (total_width, h), (255, 255, 255))

combined.paste(img_bias,  (0, 0))
combined.paste(img_var,   (img_bias.width, 0))
combined.paste(img_corr,  (img_bias.width + img_var.width, 0))

# ============================================================
# SAVE
# ============================================================
combined.save(OUTPUT_FIG, dpi=(300, 300))
print(f"✔ Combined figure saved: {OUTPUT_FIG}")
print(f"  Final size: {combined.width} x {combined.height} px")