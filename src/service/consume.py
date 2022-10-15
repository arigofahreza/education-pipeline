import hashlib
import re
import time
from datetime import datetime

from infi.clickhouse_orm import Database
from kafka import KafkaConsumer
import json

from src.config.clickhouse import CLICKHOUSE_DB, CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USERNAME, \
    CLICKHOUSE_PASSWORD
from src.model.table import Profile, Rekap


def consume():
    db = Database(CLICKHOUSE_DB, db_url=f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}', username=CLICKHOUSE_USERNAME,
                  password=CLICKHOUSE_PASSWORD)
    db.create_table(Profile)
    db.create_table(Rekap)
    rekaps = []
    profiles = []
    consumer = KafkaConsumer(
        'sc-raw-politic-geostrategic-general-001',
        bootstrap_servers=["kafka01.production02.bt:9092", "kafka02.production02.bt:9092",
                           "kafka03.production02.bt:9092", "kafka04.production02.bt:9092",
                           "kafka05.production02.bt:9092", "kafka06.production02.bt:9092"],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='pendidikan-parser-0.0.1',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for data in consumer:
        raw_data = data.value
        profile = parser(raw_data)
        data_profile = Profile(**profile)
        profiles.append(data_profile)
        if len(profiles) == 1000:
            db.insert(profiles)
            print("data bulk profile" + str(len(profiles)))
            time.sleep(2)
            profiles.clear()

        if raw_data.get('data'):
            data = raw_data.get('data')
            if data.get('Rekapitulasi'):
                for rekapitulasi in data.get('Rekapitulasi'):
                    clean_rekap = rekap(rekapitulasi)
                    clean_rekap['sekolah_id'] = data.get('sekolah_id')
                    clean_rekap['nama'] = data.get('nama')
                    data_rekap = Rekap(**clean_rekap)
                    rekaps.append(data_rekap)
                    if len(rekaps) == 1000:
                        db.insert(rekaps)
                        print("data bulk rekap" + str(len(rekaps)))
                        time.sleep(2)
                        rekaps.clear()
    if rekaps:
        db.insert(rekaps)
        print("data bulk rekap" + str(len(rekaps)))
        time.sleep(2)
        rekaps.clear()
    if profiles:
        db.insert(profiles)
        print("data bulk profile" + str(len(profiles)))
        time.sleep(2)
        profiles.clear()


def parser(raw: dict) -> dict:
    clean_profile = {}
    if raw.get('link'):
        clean_profile['link'] = raw.get('link')
    if raw.get('domain'):
        clean_profile['domain'] = raw.get('domain')
    if raw.get('data'):
        data = raw.get('data')
        if data.get('Profile'):
            for profile in data.get('Profile'):
                if profile.get('Profile User Menu'):
                    user_menu = profile.get('Profile User Menu')
                    if user_menu.get('Kepsek'):
                        clean_profile['kepala_sekolah'] = user_menu.get('Kepsek')
                    if user_menu.get('Operator'):
                        clean_profile['operator_sekolah'] = user_menu.get('Operator')
                    if user_menu.get('Akreditasi'):
                        clean_profile['akreditasi_sekolah'] = user_menu.get('Akreditasi')
                    if user_menu.get('Kurikulum'):
                        clean_profile['kurikulum_sekolah'] = user_menu.get('Kurikulum')
                    if user_menu.get('Waktu'):
                        clean_profile['waktu_sekolah'] = user_menu.get('Waktu')
                if profile.get('Identitas Sekolah'):
                    identitas = profile.get('Identitas Sekolah')
                    if identitas.get('NPSN'):
                        clean_profile['npsn'] = identitas.get('NPSN')
                    if identitas.get('Status_Kepemilikan'):
                        clean_profile['status_kepemilikan'] = identitas.get('Status_Kepemilikan')
                    if identitas.get('SK_Pendirian_Sekolah'):
                        clean_profile['sk_pendirian_sekolah'] = identitas.get('SK_Pendirian_Sekolah')
                    if identitas.get('Tanggal_SK_Izin_Operasional'):
                        clean_profile['tgl_sk_izin_operasional'] = identitas.get('Tanggal_SK_Izin_Operasional')
                    if identitas.get('SK_Izin_Operasional'):
                        clean_profile['sk_izin_operasional'] = identitas.get('SK_Izin_Operasional')
                    if identitas.get('Tanggal_SK_Pendirian'):
                        clean_profile['tgl_sk_sekolah'] = identitas.get('Tanggal_SK_Pendirian')
                if profile.get('Data Pelengkap'):
                    pelengkap = profile.get('Data Pelengkap')
                    if pelengkap.get('Kebutuhan_Khusus_Dilayani'):
                        clean_profile['kebutuhan_khusus_dilayani'] = pelengkap.get('Kebutuhan_Khusus_Dilayani')
                    if pelengkap.get('Nama_Bank'):
                        clean_profile['nama_bank'] = pelengkap.get('Nama_Bank')
                    if pelengkap.get('Cabang KCP/Unit'):
                        clean_profile['cabang_bank'] = pelengkap.get('Cabang KCP/Unit')
                    if pelengkap.get('Rekening Atas Nama'):
                        clean_profile['rekening_a_n'] = pelengkap.get('Rekening Atas Nama')
                if profile.get('Data Rinci'):
                    rinci = profile.get('Data Rinci')
                    if rinci.get('Status_BOS'):
                        clean_profile['status_bos'] = rinci.get('Status_BOS')
                    if rinci.get('Waktu_Penyelenggaraan'):
                        clean_profile['waktu_penyelenggaraan'] = rinci.get('Waktu_Penyelenggaraan')
                    if rinci.get('Sertifikasi_ISO'):
                        clean_profile['sertifikasi_iso'] = rinci.get('Sertifikasi_ISO')
                    if rinci.get('Sumber_Listrik'):
                        clean_profile['sumber_listrik'] = rinci.get('Sumber_Listrik')
                    if rinci.get('Daya_Listrik'):
                        clean_profile['daya_listrik'] = rinci.get('Daya_Listrik')
                    if rinci.get('Akses_Internet'):
                        clean_profile['akses_internet'] = rinci.get('akses_internet')

        if data.get('Kontak'):
            for kontak in data.get('Kontak'):
                if kontak.get('Kontak Utama'):
                    kontak_utama = kontak.get('Kontak Utama')
                    if kontak_utama.get('Alamat'):
                        clean_profile['alamat'] = kontak_utama.get('Alamat')
                    if kontak_utama.get('RT / RW'):
                        clean_profile['rt_rw'] = kontak_utama.get('RT / RW')
                    if kontak_utama.get('Dusun'):
                        clean_profile['dusun'] = kontak_utama.get('Dusun')
                    if kontak_utama.get('Desa / Kelurahan'):
                        clean_profile['kelurahan'] = kontak_utama.get('Desa / Kelurahan')
                    if kontak_utama.get('Kecamatan'):
                        if 'Kec. ' in kontak_utama.get('Kecamatan'):
                            clean_profile['kecamatan'] = kontak_utama.get('Kecamatan').replace('Kec. ', '').upper()
                        else:
                            clean_profile['kecamatan'] = kontak_utama.get('Kecamatan').upper()
                    if kontak_utama.get('Kabupaten'):
                        if 'Kab. ' in kontak_utama.get('Kabupaten'):
                            clean_profile['kabupaten'] = kontak_utama.get('Kabupaten').replace('Kab. ', '').upper()
                        else:
                            clean_profile['kabupaten'] = kontak_utama.get('Kabupaten').upper()
                    if kontak_utama.get('Provinsi'):
                        if 'Prov. ' in kontak_utama.get('Provinsi'):
                            clean_profile['provinsi'] = kontak_utama.get('Provinsi').replace('Prov. ', '').upper()
                        else:
                            clean_profile['provinsi'] = kontak_utama.get('Provinsi').upper()
                        clean_profile['idx'] = generate_partition_key(f'{kontak_utama.get("Provinsi")}_{kontak_utama.get("Kabupaten")}')
                    if kontak_utama.get('Kode Pos'):
                        clean_profile['kode_pos'] = kontak_utama.get('Kode Pos')
                    if kontak_utama.get('Lintang') and kontak_utama.get('Lintang') != '0':
                        clean_profile['latitude'] = float(kontak_utama.get('Lintang'))
                    if kontak_utama.get('Bujur') and kontak_utama.get('Bujur') != '0':
                        clean_profile['longitude'] = float(kontak_utama.get('Bujur'))

        if data.get('nama'):
            clean_profile['nama'] = data.get('nama')
        if data.get('sekolah_id'):
            clean_profile['sekolah_id'] = data.get('sekolah_id')
        if data.get('induk_kecamatan'):
            if 'Kec. ' in data.get('induk_kecamatan'):
                clean_profile['induk_kecamatan'] = data.get('induk_kecamatan').replace('Kec. ', '').upper()
            else:
                clean_profile['induk_kecamatan'] = data.get('induk_kecamatan').upper()
        if data.get('kode_wilayah_induk_kecamatan'):
            clean_profile['kode_wilayah_induk_kecamatan'] = data.get('kode_wilayah_induk_kecamatan').strip()
        if data.get('induk_kabupaten'):
            if 'Kab. ' in data.get('induk_kabupaten'):
                clean_profile['induk_kabupaten'] = data.get('induk_kabupaten').replace('Kab. ', '').upper()
            else:
                clean_profile['induk_kabupaten'] = data.get('induk_kabupaten').upper()
        if data.get('kode_wilayah_induk_kabupaten'):
            clean_profile['kode_wilayah_induk_kabupaten'] = data.get('kode_wilayah_induk_kabupaten').strip()
        if data.get('induk_provinsi'):
            if 'Prov. ' in data.get('induk_provinsi'):
                clean_profile['induk_provinsi'] = data.get('induk_provinsi').replace('Prov. ', '').upper()
            else:
                clean_profile['induk_provinsi'] = data.get('induk_provinsi').upper()
        if data.get('kode_wilayah_induk_provinsi'):
            clean_profile['kode_wilayah_induk_provinsi'] = data.get('kode_wilayah_induk_provinsi').strip()
        if data.get('bentuk_pendidikan'):
            clean_profile['bentuk_pendidikan'] = data.get('bentuk_pendidikan')
        if data.get('status_sekolah'):
            clean_profile['status_sekolah'] = data.get('status_sekolah')
        if data.get('sinkron_terakhir'):
            try:
                clean_profile['sinkron_terakhir'] = datetime.strptime(data.get('sinkron_terakhir'),
                                                                      "%d %b %Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
            except:
                pass
        if data.get('sekolah_id_enkrip'):
            clean_profile['sekolah_id_enkrip'] = data.get('sekolah_id_enkrip')
    clean_profile['id'] = hashlib.md5(json.dumps(clean_profile).encode()).hexdigest()
    return clean_profile


def rekap(raw: dict) -> dict:
    clean_rekap = {}

    if raw.get('semester_id'):
        clean_rekap['semester_id'] = raw.get('semester_id')
        clean_rekap['idx'] = generate_partition_key(raw.get('semester_id'))
    if raw.get('semester_name'):
        clean_rekap['semester_name'] = raw.get('semester_name')
    if raw.get('Data PTK dan PD'):
        ptk_pd = raw.get('Data PTK dan PD')
        if ptk_pd.get('Laki-laki'):
            for key, value in ptk_pd.items():
                for child_key, child_value in value.items():
                    clean_rekap[f'{child_key.lower()}_laki_laki'] = child_value
        if ptk_pd.get('Perempuan'):
            for key, value in ptk_pd.items():
                for child_key, child_value in value.items():
                    clean_rekap[f'{child_key.lower()}_perempuan'] = child_value
    if raw.get('Data Sarpras'):
        sarpras = raw.get('Data Sarpras')
        if raw.get('semester_id')[-1] == '1':
            if sarpras.get('Ruang Kelas'):
                clean_rekap['ruang_kelas'] = sarpras.get('Ruang Kelas')['Ganjil']
            if sarpras.get('Ruang Perpustakaan'):
                clean_rekap['ruang_perpustakaan'] = sarpras.get('Ruang Perpustakaan')['Ganjil']
            if sarpras.get('Ruang Laboratorium'):
                clean_rekap['ruang_laboratorium'] = sarpras.get('Ruang Laboratorium')['Ganjil']
            if sarpras.get('Ruang Praktik'):
                clean_rekap['ruang_praktik'] = sarpras.get('Ruang Praktik')['Ganjil']
            if sarpras.get('Ruang Pimpinan'):
                clean_rekap['ruang_pimpinan'] = sarpras.get('Ruang Pimpinan')['Ganjil']
            if sarpras.get('Ruang Guru'):
                clean_rekap['ruang_guru'] = sarpras.get('Ruang Guru')['Ganjil']
            if sarpras.get('Ruang Ibadah'):
                clean_rekap['ruang_ibadah'] = sarpras.get('Ruang Ibadah')['Ganjil']
            if sarpras.get('Ruang UKS'):
                clean_rekap['ruang_uks'] = sarpras.get('Ruang UKS')['Ganjil']
            if sarpras.get('Ruang Toilet'):
                clean_rekap['ruang_toilet'] = sarpras.get('Ruang Toilet')['Ganjil']
            if sarpras.get('Ruang Gudang'):
                clean_rekap['ruang_gudang'] = sarpras.get('Ruang Gudang')['Ganjil']
            if sarpras.get('Ruang Sirkulasi'):
                clean_rekap['ruang_sirkulasi'] = sarpras.get('Ruang Sirkulasi')['Ganjil']
            if sarpras.get('Tempat Bermain / Olahraga'):
                clean_rekap['tempat_bermain_olahraga'] = sarpras.get('Tempat Bermain / Olahraga')['Ganjil']
            if sarpras.get('Ruang TU'):
                clean_rekap['ruang_tu'] = sarpras.get('Ruang TU')['Ganjil']
            if sarpras.get('Ruang Konseling'):
                clean_rekap['ruang_konseling'] = sarpras.get('Ruang Konseling')['Ganjil']
            if sarpras.get('Ruang Osis'):
                clean_rekap['ruang_osis'] = sarpras.get('Ruang Osis')['Ganjil']
            if sarpras.get('Ruang Bangunan'):
                clean_rekap['ruang_bangunan'] = sarpras.get('Ruang Bangunan')['Ganjil']
            if sarpras.get('Total'):
                clean_rekap['total_ruang'] = sarpras.get('Total')['Ganjil']
        else:
            if sarpras.get('Ruang Kelas'):
                clean_rekap['ruang_kelas'] = sarpras.get('Ruang Kelas')['Genap']
            if sarpras.get('Ruang Perpustakaan'):
                clean_rekap['ruang_perpustakaan'] = sarpras.get('Ruang Perpustakaan')['Genap']
            if sarpras.get('Ruang Laboratorium'):
                clean_rekap['ruang_laboratorium'] = sarpras.get('Ruang Laboratorium')['Genap']
            if sarpras.get('Ruang Praktik'):
                clean_rekap['ruang_praktik'] = sarpras.get('Ruang Praktik')['Genap']
            if sarpras.get('Ruang Pimpinan'):
                clean_rekap['ruang_pimpinan'] = sarpras.get('Ruang Pimpinan')['Genap']
            if sarpras.get('Ruang Guru'):
                clean_rekap['ruang_guru'] = sarpras.get('Ruang Guru')['Genap']
            if sarpras.get('Ruang Ibadah'):
                clean_rekap['ruang_ibadah'] = sarpras.get('Ruang Ibadah')['Genap']
            if sarpras.get('Ruang UKS'):
                clean_rekap['ruang_uks'] = sarpras.get('Ruang UKS')['Genap']
            if sarpras.get('Ruang Toilet'):
                clean_rekap['ruang_toilet'] = sarpras.get('Ruang Toilet')['Genap']
            if sarpras.get('Ruang Gudang'):
                clean_rekap['ruang_gudang'] = sarpras.get('Ruang Gudang')['Genap']
            if sarpras.get('Ruang Sirkulasi'):
                clean_rekap['ruang_sirkulasi'] = sarpras.get('Ruang Sirkulasi')['Genap']
            if sarpras.get('Tempat Bermain / Olahraga'):
                clean_rekap['tempat_bermain_olahraga'] = sarpras.get('Tempat Bermain / Olahraga')['Genap']
            if sarpras.get('Ruang TU'):
                clean_rekap['ruang_tu'] = sarpras.get('Ruang TU')['Genap']
            if sarpras.get('Ruang Konseling'):
                clean_rekap['ruang_konseling'] = sarpras.get('Ruang Konseling')['Genap']
            if sarpras.get('Ruang Osis'):
                clean_rekap['ruang_osis'] = sarpras.get('Ruang Osis')['Genap']
            if sarpras.get('Ruang Bangunan'):
                clean_rekap['ruang_bangunan'] = sarpras.get('Ruang Bangunan')['Genap']
            if sarpras.get('Total'):
                clean_rekap['total_ruang'] = sarpras.get('Total')['Genap']
    if raw.get('Data Rombongan Belajar'):
        clean_rekap['rombel'] = int(re.findall(r'(?:sebanyak).\d', raw.get('Data Rombongan Belajar'))[0].split(' ')[1])
    clean_rekap['id'] = hashlib.md5(json.dumps(clean_rekap).encode()).hexdigest()
    return clean_rekap


def generate_partition_key(field: str) -> int:
    hm = hashlib.sha1()
    hm.update(field.encode())
    res = int(hm.hexdigest(), 16)
    return int(res % 50)
