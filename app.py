import streamlit as st

# --- Константы идеала ---
H_IDEAL = 160.0  # см
D_IDEAL = 375.0  # см
K = H_IDEAL / D_IDEAL  # Коэффициент подобия (0.4266...)

# Пределы
H_MAX = 360.0
D_MAX = 750.0

st.set_page_config(page_title="Калькулятор установки камеры", layout="centered")

st.title("📹 Пропорциональный расчет установки")
st.write(f"Идеальное соотношение: {H_IDEAL}см высоты на {D_IDEAL}см дальности.")

# --- Логика Session State для связи ползунков ---
if 'height' not in st.session_state:
    st.session_state.height = H_IDEAL
if 'dist' not in st.session_state:
    st.session_state.dist = D_IDEAL

def update_height():
    # Если двигаем дальность, пересчитываем высоту
    st.session_state.height = round(st.session_state.dist * K, 1)

def update_dist():
    # Если двигаем высоту, пересчитываем дальность
    st.session_state.dist = round(st.session_state.height / K, 1)

# --- Интерфейс ---
st.markdown("---")

# Ползунок Высоты
h_val = st.slider(
    "Высота установки от пола (см):",
    min_value=50.0,
    max_value=H_MAX,
    key='height',
    on_change=update_dist
)

# Ползунок Дальности
d_val = st.slider(
    "Дальность от двери (см):",
    min_value=100.0,
    max_value=D_MAX,
    key='dist',
    on_change=update_height
)

st.markdown("---")

# --- Результаты ---
col1, col2 = st.columns(2)
col1.metric("Итоговая высота", f"{st.session_state.height} см")
col2.metric("Итоговая дальность", f"{st.session_state.dist} см")

# Проверка ограничений
if st.session_state.height >= H_MAX:
    st.error(f"⚠️ Достигнут потолок: {H_MAX} см")
if st.session_state.dist >= D_MAX:
    st.error(f"⚠️ Достигнута макс. дальность: {D_MAX} см")

st.info(f"При этих параметрах угол обзора камеры 15° сохранит ту же перспективу, что и в 'идеале'.")
