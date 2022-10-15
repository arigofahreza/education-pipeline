from infi.clickhouse_orm import UInt64Field, DateTimeField, LowCardinalityField, StringField, Model, UInt8Field, \
    ArrayField, MergeTree, Float32Field, NullableField, DateField, Distributed, UInt32Field, Float64Field


class Profile(Model):
    id = StringField()
    idx = UInt8Field()
    link = NullableField(StringField())
    domain = NullableField(StringField())
    kepala_sekolah = NullableField(StringField())
    operator_sekolah = NullableField(StringField())
    kurikulum_sekolah = NullableField(StringField())
    akreditasi_sekolah = NullableField(StringField())
    waktu_sekolah = NullableField(StringField())
    npsn = NullableField(StringField())
    cabang_bank = NullableField(StringField())
    rekening_a_n = NullableField(StringField())
    alamat = NullableField(StringField())
    rt_rw = NullableField(StringField())
    dusun = NullableField(StringField())
    kelurahan = NullableField(StringField())
    kecamatan = NullableField(StringField())
    kabupaten = NullableField(StringField())
    provinsi = NullableField(StringField())
    kode_pos = NullableField(StringField())
    latitude = NullableField(Float32Field())
    longitude = NullableField(Float32Field())
    nama = NullableField(StringField())
    sekolah_id = NullableField(StringField())
    induk_kecamatan = NullableField(StringField())
    kode_wilayah_induk_kecamatan = NullableField(StringField())
    induk_kabupaten = NullableField(StringField())
    kode_wilayah_induk_kabupaten = NullableField(StringField())
    induk_provinsi = NullableField(StringField())
    kode_wilayah_induk_provinsi = NullableField(StringField())
    bentuk_pendidikan = NullableField(StringField())
    status_sekolah = NullableField(StringField())
    sinkron_terakhir = NullableField(StringField())
    sekolah_id_enkrip = NullableField(StringField())

    engine = MergeTree(
        partition_key=[idx],
        order_by=[id],
        primary_key=[id]
    )

    @classmethod
    def table_name(cls):
        return 'profil_pendidikan'


class Rekap(Model):
    id = StringField()
    idx = UInt8Field()
    sekolah_id = NullableField(StringField())
    nama = NullableField(StringField())
    semester_id = NullableField(StringField())
    semester_name = NullableField(StringField())
    guru_laki_laki = NullableField(UInt32Field())
    tendik_laki_laki = NullableField(UInt32Field())
    ptk_laki_laki = NullableField(UInt32Field())
    pd_laki_laki = NullableField(UInt32Field())
    guru_perempuan = NullableField(UInt32Field())
    tendik_perempuan = NullableField(UInt32Field())
    ptk_perempuan = NullableField(UInt32Field())
    pd_perempuan = NullableField(UInt32Field())
    ruang_kelas = NullableField(UInt32Field())
    ruang_perpustakaan = NullableField(UInt32Field())
    ruang_laboratorium = NullableField(UInt32Field())
    ruang_praktik = NullableField(UInt32Field())
    ruang_pimpinan = NullableField(UInt32Field())
    ruang_guru = NullableField(UInt32Field())
    ruang_ibadah = NullableField(UInt32Field())
    ruang_uks = NullableField(UInt32Field())
    ruang_toilet = NullableField(UInt32Field())
    ruang_gudang = NullableField(UInt32Field())
    ruang_sirkulasi = NullableField(UInt32Field())
    tempat_bermain_olahraga = NullableField(UInt32Field())
    ruang_tu = NullableField(UInt32Field())
    ruang_konseling = NullableField(UInt32Field())
    ruang_bangunan = NullableField(UInt32Field())
    total_ruang = NullableField(UInt32Field())
    rombel = NullableField(UInt32Field())

    engine = MergeTree(
        partition_key=[idx],
        order_by=[id],
        primary_key=[id]
    )

    @classmethod
    def table_name(cls):
        return 'rekap_pendidikan'
