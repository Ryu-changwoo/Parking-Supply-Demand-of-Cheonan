# app.py — Cheonan Parking (minimal features only)
# 남긴 것: 베이스맵(위성, 밝은지도), 라벨, [경계] 천안시/동남구/서북구/읍·면·동, [주차장] 공영/민영
# 나머지 계산/격자/점수/다운로드 등은 모두 제거 또는 주석 처리

import os
import pandas as pd
import numpy as np
from pathlib import Path
from shiny import App, ui, render, reactive
import geopandas as gpd
import folium
from folium.plugins import MiniMap
from branca.element import Element

# === 프로젝트 모듈 (필요한 것만) ===
from cheonan_mapping_core import (
    load_cheonan_boundary_shp,
    load_public_parking,
    load_private_parking,
    add_vworld_base_layers,
    _inside,
)

# -------------------------
# 경로/상수
# -------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "cheonan_data"

# 경계 SHP: 없으면 None로 두고 경계 그리기 스킵
SHP_CAND = DATA_DIR / "N3A_G0100000" / "N3A_G0100000.shp"
SHP_PATH = SHP_CAND if SHP_CAND.exists() else None

EMD_SHP_PATH = DATA_DIR / "BND_ADM_DONG_PG" / "BND_ADM_DONG_PG.shp"  # 읍·면·동(없으면 스킵)
PUBLIC_PARKING_CSV = DATA_DIR / "천안도시공사_주차장 현황_20250716.csv"
PRIVATE_PARKING_XLSX = DATA_DIR / "충청남도_천안시_민영주차장정보.xlsx"
PRIVATE_PARKING_GEO_CSV = DATA_DIR / "민영주차장_geocoded.csv"      # 있으면 우선 사용

MAP_CENTER_LAT, MAP_CENTER_LON = 36.815, 127.147
MAP_ZOOM = 12

# -------------------------
# 유틸
# -------------------------
def _inject_map_css(m):
    css = Element("""
    <style>
      .leaflet-control-container .leaflet-top.leaflet-right { right: 12px !important; left: auto !important; }
      .leaflet-control-layers { margin-top: 10px; }
    </style>
    """)
    m.get_root().html.add_child(css)

def _inside_safe(geom, lon, lat):
    if geom is None: return True
    try: return _inside(geom, lon, lat)
    except Exception: return True

def _load_emd_cheonan(emd_path: Path, cheonan_geom):
    if not Path(emd_path).exists():
        return gpd.GeoDataFrame(columns=["EMD_NAME", "geometry"], geometry="geometry", crs="EPSG:4326")
    try:
        gdf = gpd.read_file(emd_path)
        if gdf.crs is None: gdf = gdf.set_crs(epsg=4326, allow_override=True)
        else: gdf = gdf.to_crs(epsg=4326)
        # 이름 컬럼 정리
        name_col = next((c for c in ["EMD_KOR_NM","EMD_NM","법정동명","adm_nm","emd_nm"] if c in gdf.columns), None)
        gdf["EMD_NAME"] = gdf[name_col] if name_col else gdf.index.astype(str)
        out = gdf[["EMD_NAME","geometry"]].copy()
        if cheonan_geom is not None and len(out):
            out = out.loc[out.geometry.centroid.within(cheonan_geom)].copy()
        return out
    except Exception:
        return gpd.GeoDataFrame(columns=["EMD_NAME", "geometry"], geometry="geometry", crs="EPSG:4326")

# -------------------------
# UI (지도만)
# -------------------------
app_ui = ui.page_fluid(
    ui.card(
        ui.card_header("천안시 지도"),
        ui.output_ui("map_ui"),
        full_screen=True
    ),
    title="천안시 주차장 현황"
)

# -------------------------
# Server
# -------------------------
def server(input, output, session):

    @reactive.calc
    def base_data():
        bd = dict(
            cheonan_gdf=None, cheonan_geom=None, gu_map={},
            df_pub=pd.DataFrame(), df_pri=pd.DataFrame(),
            emd_gdf=gpd.GeoDataFrame(columns=["EMD_NAME","geometry"], geometry="geometry", crs="EPSG:4326")
        )

        # 경계
        try:
            if SHP_PATH is not None:
                cheonan_gdf, cheonan_geom, gu_map = load_cheonan_boundary_shp(str(SHP_PATH))
                try:
                    from shapely.validation import make_valid
                    if hasattr(cheonan_geom,"is_valid") and not cheonan_geom.is_valid:
                        cheonan_geom = make_valid(cheonan_geom)
                except Exception:
                    if hasattr(cheonan_geom,"buffer"): cheonan_geom = cheonan_geom.buffer(0)
                bd["cheonan_gdf"] = cheonan_gdf
                bd["cheonan_geom"] = cheonan_geom
                bd["gu_map"] = gu_map
        except Exception as e:
            print("[WARN] boundary load:", e)

        # 공영 주차장
        try:
            df_pub = load_public_parking(str(PUBLIC_PARKING_CSV))
            if isinstance(df_pub, pd.DataFrame) and len(df_pub):
                ren = {}
                if "위도" in df_pub.columns: ren["위도"]="lat"
                if "경도" in df_pub.columns: ren["경도"]="lon"
                if ren: df_pub = df_pub.rename(columns=ren)
                df_pub["lat"] = pd.to_numeric(df_pub.get("lat"), errors="coerce")
                df_pub["lon"] = pd.to_numeric(df_pub.get("lon"), errors="coerce")
                df_pub = df_pub.dropna(subset=["lat","lon"])
                df_pub = df_pub[df_pub.apply(lambda r: _inside_safe(bd["cheonan_geom"], r["lon"], r["lat"]), axis=1)]
                bd["df_pub"] = df_pub.reset_index(drop=True)
        except Exception as e:
            print("[WARN] public parking:", e)

        # 민영 주차장 (geocoded csv 우선, 없으면 xlsx의 lat/lon 사용)
        try:
            if Path(PRIVATE_PARKING_GEO_CSV).exists():
                df_pri = pd.read_csv(PRIVATE_PARKING_GEO_CSV)
            else:
                dfm = pd.read_excel(PRIVATE_PARKING_XLSX)
                rename = {}
                if "위도" in dfm.columns: rename["위도"]="lat"
                if "경도" in dfm.columns: rename["경도"]="lon"
                if "소재지도로명주소" in dfm.columns: rename["소재지도로명주소"]="road_address"
                if "소재지지번주소" in dfm.columns: rename["소재지지번주소"]="jibun_address"
                if "주차장명" in dfm.columns: rename["주차장명"]="name"
                df_pri = dfm.rename(columns=rename)
            if isinstance(df_pri, pd.DataFrame) and len(df_pri):
                df_pri["lat"] = pd.to_numeric(df_pri.get("lat"), errors="coerce")
                df_pri["lon"] = pd.to_numeric(df_pri.get("lon"), errors="coerce")
                df_pri = df_pri.dropna(subset=["lat","lon"])
                df_pri = df_pri[df_pri.apply(lambda r: _inside_safe(bd["cheonan_geom"], r["lon"], r["lat"]), axis=1)]
                bd["df_pri"] = df_pri.reset_index(drop=True)
        except Exception as e:
            print("[WARN] private parking:", e)

        # 읍·면·동
        try:
            bd["emd_gdf"] = _load_emd_cheonan(EMD_SHP_PATH, bd["cheonan_geom"])
        except Exception as e:
            print("[WARN] emd:", e)

        return bd

    @reactive.calc
    def build_map():
        bd = base_data()

        m = folium.Map(location=[MAP_CENTER_LAT, MAP_CENTER_LON], zoom_start=MAP_ZOOM, tiles=None)

        # 베이스맵: VWorld → 실패 시 Esri/Carto
        try:
            add_vworld_base_layers(m)  # 위성 + 라벨(VWorld Hybrid)
            # 밝은 지도(보조)
            folium.TileLayer("CartoDB positron", name="밝은 지도 (Carto Positron)", show=False).add_to(m)
        except Exception:
            folium.TileLayer(tiles="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
                             name="OSM 기본", attr="&copy;OSM", show=False).add_to(m)
            folium.TileLayer("CartoDB positron", name="밝은 지도 (Carto Positron)", show=True).add_to(m)
            folium.TileLayer("Esri.WorldImagery", name="위성 (Esri)", show=False).add_to(m)

        _inject_map_css(m)
        m.add_child(MiniMap(position="bottomright", toggle_display=True))

        # [경계] 천안시 (halo + 본선)
        try:
            if bd["cheonan_gdf"] is not None:
                folium.GeoJson(
                    bd["cheonan_gdf"],
                    name="[경계] 천안시 (halo)",
                    style_function=lambda x: {"color":"#FFFFFF","weight":7,"opacity":0.9},
                    control=False,
                ).add_to(m)
                folium.GeoJson(
                    bd["cheonan_gdf"],
                    name="[경계] 천안시",
                    style_function=lambda x: {"color":"#00E5FF","weight":3.5,"opacity":1.0},
                    highlight_function=lambda x: {"weight":5,"color":"#FFFFFF"},
                ).add_to(m)
        except Exception as e:
            print("[WARN] draw city:", e)

        # [경계] 동남구/서북구
        try:
            gm = bd.get("gu_map") or {}
            if gm.get("동남구") is not None and len(gm["동남구"]):
                folium.GeoJson(
                    gm["동남구"], name="[경계] 동남구",
                    style_function=lambda x: {"color":"#BA2FE5","weight":3,"opacity":1.0},
                    highlight_function=lambda x: {"weight":4,"color":"#FFFFFF"},
                ).add_to(m)
            if gm.get("서북구") is not None and len(gm["서북구"]):
                folium.GeoJson(
                    gm["서북구"], name="[경계] 서북구",
                    style_function=lambda x: {"color":"#FF5722","weight":3,"opacity":1.0},
                    highlight_function=lambda x: {"weight":4,"color":"#FFFFFF"},
                ).add_to(m)
        except Exception as e:
            print("[WARN] draw gus:", e)

        # [경계] 읍·면·동
        try:
            emd = bd.get("emd_gdf")
            if emd is not None and len(emd):
                emd_group = folium.FeatureGroup(name="[경계] 읍·면·동", show=True)
                folium.GeoJson(
                    emd,
                    style_function=lambda x: {"color":"#FFFFFF","weight":6.5,"opacity":1.0,"fill":False},
                    control=False,
                ).add_to(emd_group)
                folium.GeoJson(
                    emd,
                    style_function=lambda x: {"color":"#222222","weight":3.0,"opacity":1.0,"fill":False},
                    highlight_function=lambda x: {"weight":3.6,"color":"#000000"},
                    tooltip=folium.GeoJsonTooltip(fields=["EMD_NAME"], aliases=["읍·면·동:"], sticky=True),
                    control=False,
                ).add_to(emd_group)
                emd_group.add_to(m)
        except Exception as e:
            print("[WARN] draw emd:", e)

        # [주차장] 공영(파랑)
        try:
            df_pub = bd["df_pub"]
            if isinstance(df_pub, pd.DataFrame) and len(df_pub):
                fg_pub = folium.FeatureGroup(name="[주차장] 공영 (파랑)", show=True)
                for _, r in df_pub.iterrows():
                    lat = float(r.get("lat", np.nan)); lon = float(r.get("lon", np.nan))
                    if np.isnan(lat) or np.isnan(lon): continue
                    name = str(r.get("name", r.get("주차장명", "공영주차장")))
                    folium.Marker(
                        [lat, lon], tooltip=name,
                        popup=folium.Popup(f"<b>{name}</b>", max_width=300),
                        icon=folium.Icon(color="blue", icon="car", prefix="fa")
                    ).add_to(fg_pub)
                fg_pub.add_to(m)
        except Exception as e:
            print("[WARN] draw pub:", e)

        # [주차장] 민영(빨강)
        try:
            df_pri = bd["df_pri"]
            if isinstance(df_pri, pd.DataFrame) and len(df_pri):
                fg_pri = folium.FeatureGroup(name="[주차장] 민영 (빨강)", show=True)
                for _, r in df_pri.iterrows():
                    lat = float(r.get("lat", np.nan)); lon = float(r.get("lon", np.nan))
                    if np.isnan(lat) or np.isnan(lon): continue
                    name = str(r.get("name", r.get("주차장명", "민영주차장")))
                    folium.Marker(
                        [lat, lon], tooltip=name,
                        popup=folium.Popup(f"<b>{name}</b>", max_width=300),
                        icon=folium.Icon(color="red", icon="car", prefix="fa")
                    ).add_to(fg_pri)
                fg_pri.add_to(m)
        except Exception as e:
            print("[WARN] draw pri:", e)

        folium.LayerControl(collapsed=False, position="topright").add_to(m)
        return m

    @output
    @render.ui
    def map_ui():
        m = build_map()
        return ui.div(
            ui.HTML(m._repr_html_()),
            style="height: 80vh; min-height: 600px; border-radius: 8px; overflow: hidden;"
        )

app = App(app_ui, server)
