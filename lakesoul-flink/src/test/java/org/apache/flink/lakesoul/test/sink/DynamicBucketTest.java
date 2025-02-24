package org.apache.flink.lakesoul.test.sink;

import com.dmetasoul.lakesoul.meta.DBManager;
import org.apache.flink.lakesoul.test.AbstractTestBase;
import org.apache.flink.lakesoul.test.flinkSource.TestUtils;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.lakesoul.test.flinkSource.TestUtils.BATCH_TYPE;

public class DynamicBucketTest extends AbstractTestBase {


    public static void main(String[] args) {
        new DBManager().cleanMeta();
        TableEnvironment batchEnv = TestUtils.createTableEnv(BATCH_TYPE);
        batchEnv.executeSql("create database if not exists qara_mid");
        batchEnv.executeSql("create database if not exists czods");

        createParquetSource(batchEnv);
        System.out.println("createParquetSource done");
//        batchEnv.executeSql("DESCRIBE `default_catalog`.`default_database`.`mid_qara_320020_p25hz_fillup`").print();
//
//        batchEnv.executeSql("select row_number() OVER(PARTITION BY file_no order by `time`,time_series asc ) as row_id_add,*\n" +
//                "    from  `default_catalog`.`default_database`.`mid_qara_320020_p25hz_fillup`\n" +
//                "    where  flt_dt='20241217'  and file_no='3659647' ").print();

        createLakeSoulSinkTable(batchEnv);
        System.out.println("createLakeSoulSinkTable done");
//        batchEnv.executeSql("select * from `default_catalog`.`default_database`.`mid_qara_320020_p25hz_fillup`").print();
        batchEnv.executeSql("insert into `czods`.`s_qara_320020_p25hz` \n" +
                "select * from `default_catalog`.`default_database`.`mid_qara_320020_p25hz_fillup`").print();
        System.out.println("`czods`.`s_qara_320020_p25hz` ready");

        submitJob(batchEnv);
        System.out.println("submitJob done");

        batchEnv.executeSql("select count(*) from qara_mid.mid_qara_320020_p25hz_fillup").print();
    }

    static void submitJob(TableEnvironment batchEnv) {
        batchEnv.executeSql("insert into  qara_mid.mid_qara_320020_p25hz_fillup\n" +
                "select t0.`time`,t0.time_series,\n" +
                "COALESCE(t0.acms_fp,t1.acms_fp,t2.acms_fp,t3.acms_fp) as acms_fp,\n" +
                "COALESCE(t0.acv_b_on2,t1.acv_b_on2,t2.acv_b_on2,t3.acv_b_on2) as acv_b_on2,\n" +
                "COALESCE(t0.ac_iden,t1.ac_iden,t2.ac_iden,t3.ac_iden) as ac_iden,\n" +
                "COALESCE(t0.ac_tail123,t1.ac_tail123,t2.ac_tail123,t3.ac_tail123) as ac_tail123,\n" +
                "COALESCE(t0.ac_tail4,t1.ac_tail4,t2.ac_tail4,t3.ac_tail4) as ac_tail4,\n" +
                "COALESCE(t0.ac_tail456,t1.ac_tail456,t2.ac_tail456,t3.ac_tail456) as ac_tail456,\n" +
                "COALESCE(t0.ac_tail7,t1.ac_tail7,t2.ac_tail7,t3.ac_tail7) as ac_tail7,\n" +
                "COALESCE(t0.ac_typ,t1.ac_typ,t2.ac_typ,t3.ac_typ) as ac_typ,\n" +
                "COALESCE(t0.ail_lh_avl_b,t1.ail_lh_avl_b,t2.ail_lh_avl_b,t3.ail_lh_avl_b) as ail_lh_avl_b,\n" +
                "COALESCE(t0.ail_lh_avl_g,t1.ail_lh_avl_g,t2.ail_lh_avl_g,t3.ail_lh_avl_g) as ail_lh_avl_g,\n" +
                "COALESCE(t0.ail_rh_avl_b,t1.ail_rh_avl_b,t2.ail_rh_avl_b,t3.ail_rh_avl_b) as ail_rh_avl_b,\n" +
                "COALESCE(t0.ail_rh_avl_g,t1.ail_rh_avl_g,t2.ail_rh_avl_g,t3.ail_rh_avl_g) as ail_rh_avl_g,\n" +
                "COALESCE(t0.airline_id,t1.airline_id,t2.airline_id,t3.airline_id) as airline_id,\n" +
                "COALESCE(t0.airl_idt,t1.airl_idt,t2.airl_idt,t3.airl_idt) as airl_idt,\n" +
                "COALESCE(t0.air_ground,t1.air_ground,t2.air_ground,t3.air_ground) as air_ground,\n" +
                "COALESCE(t0.aiw_rw,t1.aiw_rw,t2.aiw_rw,t3.aiw_rw) as aiw_rw,\n" +
                "COALESCE(t0.alt_cpt,t1.alt_cpt,t2.alt_cpt,t3.alt_cpt) as alt_cpt,\n" +
                "COALESCE(t0.alt_fo,t1.alt_fo,t2.alt_fo,t3.alt_fo) as alt_fo,\n" +
                "COALESCE(t0.alt_swc,t1.alt_swc,t2.alt_swc,t3.alt_swc) as alt_swc,\n" +
                "COALESCE(t0.apr_ctl,t1.apr_ctl,t2.apr_ctl,t3.apr_ctl) as apr_ctl,\n" +
                "COALESCE(t0.apubld_vv_op,t1.apubld_vv_op,t2.apubld_vv_op,t3.apubld_vv_op) as apubld_vv_op,\n" +
                "COALESCE(t0.aspd_ctl,t1.aspd_ctl,t2.aspd_ctl,t3.aspd_ctl) as aspd_ctl,\n" +
                "COALESCE(t0.aud_tr_fail,t1.aud_tr_fail,t2.aud_tr_fail,t3.aud_tr_fail) as aud_tr_fail,\n" +
                "COALESCE(t0.bld_vlv1,t1.bld_vlv1,t2.bld_vlv1,t3.bld_vlv1) as bld_vlv1,\n" +
                "COALESCE(t0.bld_vlv2,t1.bld_vlv2,t2.bld_vlv2,t3.bld_vlv2) as bld_vlv2,\n" +
                "COALESCE(t0.bp1,t1.bp1,t2.bp1,t3.bp1) as bp1,\n" +
                "COALESCE(t0.bp2,t1.bp2,t2.bp2,t3.bp2) as bp2,\n" +
                "COALESCE(t0.cent_curr,t1.cent_curr,t2.cent_curr,t3.cent_curr) as cent_curr,\n" +
                "COALESCE(t0.city_from,t1.city_from,t2.city_from,t3.city_from) as city_from,\n" +
                "COALESCE(t0.city_from_r,t1.city_from_r,t2.city_from_r,t3.city_from_r) as city_from_r,\n" +
                "COALESCE(t0.city_to_r,t1.city_to_r,t2.city_to_r,t3.city_to_r) as city_to_r,\n" +
                "COALESCE(t0.ck_conf_scl_mx,t1.ck_conf_scl_mx,t2.ck_conf_scl_mx,t3.ck_conf_scl_mx) as ck_conf_scl_mx,\n" +
                "COALESCE(t0.ck_egt_scl_max,t1.ck_egt_scl_max,t2.ck_egt_scl_max,t3.ck_egt_scl_max) as ck_egt_scl_max,\n" +
                "COALESCE(t0.ck_epr_scl_max,t1.ck_epr_scl_max,t2.ck_epr_scl_max,t3.ck_epr_scl_max) as ck_epr_scl_max,\n" +
                "COALESCE(t0.ck_ins_eng_mod,t1.ck_ins_eng_mod,t2.ck_ins_eng_mod,t3.ck_ins_eng_mod) as ck_ins_eng_mod,\n" +
                "COALESCE(t0.ck_itt_scl_max,t1.ck_itt_scl_max,t2.ck_itt_scl_max,t3.ck_itt_scl_max) as ck_itt_scl_max,\n" +
                "COALESCE(t0.ck_ldg_nr,t1.ck_ldg_nr,t2.ck_ldg_nr,t3.ck_ldg_nr) as ck_ldg_nr,\n" +
                "COALESCE(t0.ck_n1_scl_max,t1.ck_n1_scl_max,t2.ck_n1_scl_max,t3.ck_n1_scl_max) as ck_n1_scl_max,\n" +
                "COALESCE(t0.ck_tla_scl_max,t1.ck_tla_scl_max,t2.ck_tla_scl_max,t3.ck_tla_scl_max) as ck_tla_scl_max,\n" +
                "COALESCE(t0.ck_tla_scl_min,t1.ck_tla_scl_min,t2.ck_tla_scl_min,t3.ck_tla_scl_min) as ck_tla_scl_min,\n" +
                "COALESCE(t0.ck_torq_scl_mx,t1.ck_torq_scl_mx,t2.ck_torq_scl_mx,t3.ck_torq_scl_mx) as ck_torq_scl_mx,\n" +
                "COALESCE(t0.cmc_tr_fail,t1.cmc_tr_fail,t2.cmc_tr_fail,t3.cmc_tr_fail) as cmc_tr_fail,\n" +
                "COALESCE(t0.dat_day,t1.dat_day,t2.dat_day,t3.dat_day) as dat_day,\n" +
                "COALESCE(t0.dat_month,t1.dat_month,t2.dat_month,t3.dat_month) as dat_month,\n" +
                "COALESCE(t0.day_curr,t1.day_curr,t2.day_curr,t3.day_curr) as day_curr,\n" +
                "COALESCE(t0.db_upd_cyc,t1.db_upd_cyc,t2.db_upd_cyc,t3.db_upd_cyc) as db_upd_cyc,\n" +
                "COALESCE(t0.db_upd_date,t1.db_upd_date,t2.db_upd_date,t3.db_upd_date) as db_upd_date,\n" +
                "COALESCE(t0.dc1_bus_on,t1.dc1_bus_on,t2.dc1_bus_on,t3.dc1_bus_on) as dc1_bus_on,\n" +
                "COALESCE(t0.dc2_bus_on,t1.dc2_bus_on,t2.dc2_bus_on,t3.dc2_bus_on) as dc2_bus_on,\n" +
                "COALESCE(t0.dec_height,t1.dec_height,t2.dec_height,t3.dec_height) as dec_height,\n" +
                "COALESCE(t0.dfc,t1.dfc,t2.dfc,t3.dfc) as dfc,\n" +
                "COALESCE(t0.dits_fail,t1.dits_fail,t2.dits_fail,t3.dits_fail) as dits_fail,\n" +
                "COALESCE(t0.dmc_1_invald,t1.dmc_1_invald,t2.dmc_1_invald,t3.dmc_1_invald) as dmc_1_invald,\n" +
                "COALESCE(t0.dmc_2_invald,t1.dmc_2_invald,t2.dmc_2_invald,t3.dmc_2_invald) as dmc_2_invald,\n" +
                "COALESCE(t0.dmc_3_invald,t1.dmc_3_invald,t2.dmc_3_invald,t3.dmc_3_invald) as dmc_3_invald,\n" +
                "COALESCE(t0.dmc_3_xfr_ca,t1.dmc_3_xfr_ca,t2.dmc_3_xfr_ca,t3.dmc_3_xfr_ca) as dmc_3_xfr_ca,\n" +
                "COALESCE(t0.dmc_3_xfr_fo,t1.dmc_3_xfr_fo,t2.dmc_3_xfr_fo,t3.dmc_3_xfr_fo) as dmc_3_xfr_fo,\n" +
                "COALESCE(t0.dmc_mark_id,t1.dmc_mark_id,t2.dmc_mark_id,t3.dmc_mark_id) as dmc_mark_id,\n" +
                "COALESCE(t0.dme_dis1,t1.dme_dis1,t2.dme_dis1,t3.dme_dis1) as dme_dis1,\n" +
                "COALESCE(t0.dme_dis2,t1.dme_dis2,t2.dme_dis2,t3.dme_dis2) as dme_dis2,\n" +
                "COALESCE(t0.dme_frq1,t1.dme_frq1,t2.dme_frq1,t3.dme_frq1) as dme_frq1,\n" +
                "COALESCE(t0.dme_frq2,t1.dme_frq2,t2.dme_frq2,t3.dme_frq2) as dme_frq2,\n" +
                "COALESCE(t0.drift,t1.drift,t2.drift,t3.drift) as drift,\n" +
                "COALESCE(t0.ecamdu_1_off,t1.ecamdu_1_off,t2.ecamdu_1_off,t3.ecamdu_1_off) as ecamdu_1_off,\n" +
                "COALESCE(t0.ecamdu_2_off,t1.ecamdu_2_off,t2.ecamdu_2_off,t3.ecamdu_2_off) as ecamdu_2_off,\n" +
                "COALESCE(t0.ecamnd_xfrca,t1.ecamnd_xfrca,t2.ecamnd_xfrca,t3.ecamnd_xfrca) as ecamnd_xfrca,\n" +
                "COALESCE(t0.ecamnd_xfrfo,t1.ecamnd_xfrfo,t2.ecamnd_xfrfo,t3.ecamnd_xfrfo) as ecamnd_xfrfo,\n" +
                "COALESCE(t0.ecam_sel_mat,t1.ecam_sel_mat,t2.ecam_sel_mat,t3.ecam_sel_mat) as ecam_sel_mat,\n" +
                "COALESCE(t0.ecu_eec_1_cb,t1.ecu_eec_1_cb,t2.ecu_eec_1_cb,t3.ecu_eec_1_cb) as ecu_eec_1_cb,\n" +
                "COALESCE(t0.ecu_eec_2_cb,t1.ecu_eec_2_cb,t2.ecu_eec_2_cb,t3.ecu_eec_2_cb) as ecu_eec_2_cb,\n" +
                "COALESCE(t0.egp1,t1.egp1,t2.egp1,t3.egp1) as egp1,\n" +
                "COALESCE(t0.egp2,t1.egp2,t2.egp2,t3.egp2) as egp2,\n" +
                "COALESCE(t0.elac1_pt_flt,t1.elac1_pt_flt,t2.elac1_pt_flt,t3.elac1_pt_flt) as elac1_pt_flt,\n" +
                "COALESCE(t0.elac1_ro_flt,t1.elac1_ro_flt,t2.elac1_ro_flt,t3.elac1_ro_flt) as elac1_ro_flt,\n" +
                "COALESCE(t0.elac2_pt_flt,t1.elac2_pt_flt,t2.elac2_pt_flt,t3.elac2_pt_flt) as elac2_pt_flt,\n" +
                "COALESCE(t0.elac2_rl_flt,t1.elac2_rl_flt,t2.elac2_rl_flt,t3.elac2_rl_flt) as elac2_rl_flt,\n" +
                "COALESCE(t0.elac_1_fault,t1.elac_1_fault,t2.elac_1_fault,t3.elac_1_fault) as elac_1_fault,\n" +
                "COALESCE(t0.elac_2_fault,t1.elac_2_fault,t2.elac_2_fault,t3.elac_2_fault) as elac_2_fault,\n" +
                "COALESCE(t0.eng1_pt25,t1.eng1_pt25,t2.eng1_pt25,t3.eng1_pt25) as eng1_pt25,\n" +
                "COALESCE(t0.eng2_pt25,t1.eng2_pt25,t2.eng2_pt25,t3.eng2_pt25) as eng2_pt25,\n" +
                "COALESCE(t0.epr_max,t1.epr_max,t2.epr_max,t3.epr_max) as epr_max,\n" +
                "COALESCE(t0.ext_tmp_eng1,t1.ext_tmp_eng1,t2.ext_tmp_eng1,t3.ext_tmp_eng1) as ext_tmp_eng1,\n" +
                "COALESCE(t0.ext_tmp_eng2,t1.ext_tmp_eng2,t2.ext_tmp_eng2,t3.ext_tmp_eng2) as ext_tmp_eng2,\n" +
                "COALESCE(t0.fdiu_fail,t1.fdiu_fail,t2.fdiu_fail,t3.fdiu_fail) as fdiu_fail,\n" +
                "COALESCE(t0.fdiu_prg_id,t1.fdiu_prg_id,t2.fdiu_prg_id,t3.fdiu_prg_id) as fdiu_prg_id,\n" +
                "COALESCE(t0.fdr_fail,t1.fdr_fail,t2.fdr_fail,t3.fdr_fail) as fdr_fail,\n" +
                "COALESCE(t0.fdr_pb_fail,t1.fdr_pb_fail,t2.fdr_pb_fail,t3.fdr_pb_fail) as fdr_pb_fail,\n" +
                "COALESCE(t0.flap_faul,t1.flap_faul,t2.flap_faul,t3.flap_faul) as flap_faul,\n" +
                "COALESCE(t0.flap_lever_c,t1.flap_lever_c,t2.flap_lever_c,t3.flap_lever_c) as flap_lever_c,\n" +
                "COALESCE(t0.flare,t1.flare,t2.flare,t3.flare) as flare,\n" +
                "COALESCE(t0.fleet_idt,t1.fleet_idt,t2.fleet_idt,t3.fleet_idt) as fleet_idt,\n" +
                "COALESCE(t0.flight_phas0,t1.flight_phas0,t2.flight_phas0,t3.flight_phas0) as flight_phas0,\n" +
                "COALESCE(t0.fltnum,t1.fltnum,t2.fltnum,t3.fltnum) as fltnum,\n" +
                "COALESCE(t0.fwc_valid,t1.fwc_valid,t2.fwc_valid,t3.fwc_valid) as fwc_valid,\n" +
                "COALESCE(t0.gw,t1.gw,t2.gw,t3.gw) as gw,\n" +
                "COALESCE(t0.head_selon,t1.head_selon,t2.head_selon,t3.head_selon) as head_selon,\n" +
                "COALESCE(t0.hpv_nfc_1,t1.hpv_nfc_1,t2.hpv_nfc_1,t3.hpv_nfc_1) as hpv_nfc_1,\n" +
                "COALESCE(t0.hpv_nfc_2,t1.hpv_nfc_2,t2.hpv_nfc_2,t3.hpv_nfc_2) as hpv_nfc_2,\n" +
                "COALESCE(t0.ident_eng1,t1.ident_eng1,t2.ident_eng1,t3.ident_eng1) as ident_eng1,\n" +
                "COALESCE(t0.ident_eng2,t1.ident_eng2,t2.ident_eng2,t3.ident_eng2) as ident_eng2,\n" +
                "COALESCE(t0.ils_frq1,t1.ils_frq1,t2.ils_frq1,t3.ils_frq1) as ils_frq1,\n" +
                "COALESCE(t0.ils_frq2,t1.ils_frq2,t2.ils_frq2,t3.ils_frq2) as ils_frq2,\n" +
                "COALESCE(t0.isov1,t1.isov1,t2.isov1,t3.isov1) as isov1,\n" +
                "COALESCE(t0.isov2,t1.isov2,t2.isov2,t3.isov2) as isov2,\n" +
                "COALESCE(t0.ivv_c,t1.ivv_c,t2.ivv_c,t3.ivv_c) as ivv_c,\n" +
                "COALESCE(t0.landing_roll,t1.landing_roll,t2.landing_roll,t3.landing_roll) as landing_roll,\n" +
                "COALESCE(t0.latp,t1.latp,t2.latp,t3.latp) as latp,\n" +
                "COALESCE(t0.lat_ck_fail,t1.lat_ck_fail,t2.lat_ck_fail,t3.lat_ck_fail) as lat_ck_fail,\n" +
                "COALESCE(t0.lat_ck_n_r,t1.lat_ck_n_r,t2.lat_ck_n_r,t3.lat_ck_n_r) as lat_ck_n_r,\n" +
                "COALESCE(t0.ldg_seldw,t1.ldg_seldw,t2.ldg_seldw,t3.ldg_seldw) as ldg_seldw,\n" +
                "COALESCE(t0.ldg_selup,t1.ldg_selup,t2.ldg_selup,t3.ldg_selup) as ldg_selup,\n" +
                "COALESCE(t0.ldg_sel_up,t1.ldg_sel_up,t2.ldg_sel_up,t3.ldg_sel_up) as ldg_sel_up,\n" +
                "COALESCE(t0.long_ck_fail,t1.long_ck_fail,t2.long_ck_fail,t3.long_ck_fail) as long_ck_fail,\n" +
                "COALESCE(t0.long_ck_n_r,t1.long_ck_n_r,t2.long_ck_n_r,t3.long_ck_n_r) as long_ck_n_r,\n" +
                "COALESCE(t0.lonp,t1.lonp,t2.lonp,t3.lonp) as lonp,\n" +
                "COALESCE(t0.mach_selon,t1.mach_selon,t2.mach_selon,t3.mach_selon) as mach_selon,\n" +
                "COALESCE(t0.mls_select,t1.mls_select,t2.mls_select,t3.mls_select) as mls_select,\n" +
                "COALESCE(t0.mode_mmr1,t1.mode_mmr1,t2.mode_mmr1,t3.mode_mmr1) as mode_mmr1,\n" +
                "COALESCE(t0.mode_mmr2,t1.mode_mmr2,t2.mode_mmr2,t3.mode_mmr2) as mode_mmr2,\n" +
                "COALESCE(t0.mode_nd_capt,t1.mode_nd_capt,t2.mode_nd_capt,t3.mode_nd_capt) as mode_nd_capt,\n" +
                "COALESCE(t0.mode_nd_fo,t1.mode_nd_fo,t2.mode_nd_fo,t3.mode_nd_fo) as mode_nd_fo,\n" +
                "COALESCE(t0.month_curr,t1.month_curr,t2.month_curr,t3.month_curr) as month_curr,\n" +
                "COALESCE(t0.n1_epr_tgt1,t1.n1_epr_tgt1,t2.n1_epr_tgt1,t3.n1_epr_tgt1) as n1_epr_tgt1,\n" +
                "COALESCE(t0.nd_ca_an_off,t1.nd_ca_an_off,t2.nd_ca_an_off,t3.nd_ca_an_off) as nd_ca_an_off,\n" +
                "COALESCE(t0.nd_fo_an_off,t1.nd_fo_an_off,t2.nd_fo_an_off,t3.nd_fo_an_off) as nd_fo_an_off,\n" +
                "COALESCE(t0.nm_range_ca,t1.nm_range_ca,t2.nm_range_ca,t3.nm_range_ca) as nm_range_ca,\n" +
                "COALESCE(t0.nm_range_fo,t1.nm_range_fo,t2.nm_range_fo,t3.nm_range_fo) as nm_range_fo,\n" +
                "COALESCE(t0.norm_ck_fail,t1.norm_ck_fail,t2.norm_ck_fail,t3.norm_ck_fail) as norm_ck_fail,\n" +
                "COALESCE(t0.norm_ck_n_r,t1.norm_ck_n_r,t2.norm_ck_n_r,t3.norm_ck_n_r) as norm_ck_n_r,\n" +
                "COALESCE(t0.no_data_eec1,t1.no_data_eec1,t2.no_data_eec1,t3.no_data_eec1) as no_data_eec1,\n" +
                "COALESCE(t0.no_data_eec2,t1.no_data_eec2,t2.no_data_eec2,t3.no_data_eec2) as no_data_eec2,\n" +
                "COALESCE(t0.oil_prs1,t1.oil_prs1,t2.oil_prs1,t3.oil_prs1) as oil_prs1,\n" +
                "COALESCE(t0.oil_prs2,t1.oil_prs2,t2.oil_prs2,t3.oil_prs2) as oil_prs2,\n" +
                "COALESCE(t0.oil_tmp1,t1.oil_tmp1,t2.oil_tmp1,t3.oil_tmp1) as oil_tmp1,\n" +
                "COALESCE(t0.oil_tmp2,t1.oil_tmp2,t2.oil_tmp2,t3.oil_tmp2) as oil_tmp2,\n" +
                "COALESCE(t0.oiq1,t1.oiq1,t2.oiq1,t3.oiq1) as oiq1,\n" +
                "COALESCE(t0.oiq2,t1.oiq2,t2.oiq2,t3.oiq2) as oiq2,\n" +
                "COALESCE(t0.opv_nfo_1,t1.opv_nfo_1,t2.opv_nfo_1,t3.opv_nfo_1) as opv_nfo_1,\n" +
                "COALESCE(t0.opv_nfo_2,t1.opv_nfo_2,t2.opv_nfo_2,t3.opv_nfo_2) as opv_nfo_2,\n" +
                "COALESCE(t0.pfdnd_xfr_ca,t1.pfdnd_xfr_ca,t2.pfdnd_xfr_ca,t3.pfdnd_xfr_ca) as pfdnd_xfr_ca,\n" +
                "COALESCE(t0.pfdnd_xfr_fo,t1.pfdnd_xfr_fo,t2.pfdnd_xfr_fo,t3.pfdnd_xfr_fo) as pfdnd_xfr_fo,\n" +
                "COALESCE(t0.pfd_ca_anoff,t1.pfd_ca_anoff,t2.pfd_ca_anoff,t3.pfd_ca_anoff) as pfd_ca_anoff,\n" +
                "COALESCE(t0.pfd_fo_anoff,t1.pfd_fo_anoff,t2.pfd_fo_anoff,t3.pfd_fo_anoff) as pfd_fo_anoff,\n" +
                "COALESCE(t0.pitch_altr1,t1.pitch_altr1,t2.pitch_altr1,t3.pitch_altr1) as pitch_altr1,\n" +
                "COALESCE(t0.pitch_altr2,t1.pitch_altr2,t2.pitch_altr2,t3.pitch_altr2) as pitch_altr2,\n" +
                "COALESCE(t0.pitch_dir_lw,t1.pitch_dir_lw,t2.pitch_dir_lw,t3.pitch_dir_lw) as pitch_dir_lw,\n" +
                "COALESCE(t0.pitch_nor,t1.pitch_nor,t2.pitch_nor,t3.pitch_nor) as pitch_nor,\n" +
                "COALESCE(t0.playbk_fail,t1.playbk_fail,t2.playbk_fail,t3.playbk_fail) as playbk_fail,\n" +
                "COALESCE(t0.playbk_inact,t1.playbk_inact,t2.playbk_inact,t3.playbk_inact) as playbk_inact,\n" +
                "COALESCE(t0.prv_nfc_1,t1.prv_nfc_1,t2.prv_nfc_1,t3.prv_nfc_1) as prv_nfc_1,\n" +
                "COALESCE(t0.prv_nfc_2,t1.prv_nfc_2,t2.prv_nfc_2,t3.prv_nfc_2) as prv_nfc_2,\n" +
                "COALESCE(t0.qar_fail,t1.qar_fail,t2.qar_fail,t3.qar_fail) as qar_fail,\n" +
                "COALESCE(t0.qar_tapelow,t1.qar_tapelow,t2.qar_tapelow,t3.qar_tapelow) as qar_tapelow,\n" +
                "COALESCE(t0.sdac1_valid,t1.sdac1_valid,t2.sdac1_valid,t3.sdac1_valid) as sdac1_valid,\n" +
                "COALESCE(t0.sdac2_valid,t1.sdac2_valid,t2.sdac2_valid,t3.sdac2_valid) as sdac2_valid,\n" +
                "COALESCE(t0.sec_1_fault,t1.sec_1_fault,t2.sec_1_fault,t3.sec_1_fault) as sec_1_fault,\n" +
                "COALESCE(t0.sec_2_fault,t1.sec_2_fault,t2.sec_2_fault,t3.sec_2_fault) as sec_2_fault,\n" +
                "COALESCE(t0.sec_3_fault,t1.sec_3_fault,t2.sec_3_fault,t3.sec_3_fault) as sec_3_fault,\n" +
                "COALESCE(t0.slat_faul,t1.slat_faul,t2.slat_faul,t3.slat_faul) as slat_faul,\n" +
                "COALESCE(t0.spd_brk,t1.spd_brk,t2.spd_brk,t3.spd_brk) as spd_brk,\n" +
                "COALESCE(t0.stab_bld_pos1,t1.stab_bld_pos1,t2.stab_bld_pos1,t3.stab_bld_pos1) as stab_bld_pos1,\n" +
                "COALESCE(t0.stab_bld_pos2,t1.stab_bld_pos2,t2.stab_bld_pos2,t3.stab_bld_pos2) as stab_bld_pos2,\n" +
                "COALESCE(t0.start_vlv1,t1.start_vlv1,t2.start_vlv1,t3.start_vlv1) as start_vlv1,\n" +
                "COALESCE(t0.start_vlv2,t1.start_vlv2,t2.start_vlv2,t3.start_vlv2) as start_vlv2,\n" +
                "COALESCE(t0.sta_van_eng1,t1.sta_van_eng1,t2.sta_van_eng1,t3.sta_van_eng1) as sta_van_eng1,\n" +
                "COALESCE(t0.sta_van_eng2,t1.sta_van_eng2,t2.sta_van_eng2,t3.sta_van_eng2) as sta_van_eng2,\n" +
                "COALESCE(t0.tcas_sens_1,t1.tcas_sens_1,t2.tcas_sens_1,t3.tcas_sens_1) as tcas_sens_1,\n" +
                "COALESCE(t0.tcas_sens_2,t1.tcas_sens_2,t2.tcas_sens_2,t3.tcas_sens_2) as tcas_sens_2,\n" +
                "COALESCE(t0.tcas_sens_3,t1.tcas_sens_3,t2.tcas_sens_3,t3.tcas_sens_3) as tcas_sens_3,\n" +
                "COALESCE(t0.tla1_c,t1.tla1_c,t2.tla1_c,t3.tla1_c) as tla1_c,\n" +
                "COALESCE(t0.tla2_c,t1.tla2_c,t2.tla2_c,t3.tla2_c) as tla2_c,\n" +
                "COALESCE(t0.touch_and_go,t1.touch_and_go,t2.touch_and_go,t3.touch_and_go) as touch_and_go,\n" +
                "COALESCE(t0.trb1_cool,t1.trb1_cool,t2.trb1_cool,t3.trb1_cool) as trb1_cool,\n" +
                "COALESCE(t0.trb2_cool,t1.trb2_cool,t2.trb2_cool,t3.trb2_cool) as trb2_cool,\n" +
                "COALESCE(t0.tt25_l,t1.tt25_l,t2.tt25_l,t3.tt25_l) as tt25_l,\n" +
                "COALESCE(t0.tt25_r,t1.tt25_r,t2.tt25_r,t3.tt25_r) as tt25_r,\n" +
                "COALESCE(t0.t_inl_prs1,t1.t_inl_prs1,t2.t_inl_prs1,t3.t_inl_prs1) as t_inl_prs1,\n" +
                "COALESCE(t0.t_inl_prs2,t1.t_inl_prs2,t2.t_inl_prs2,t3.t_inl_prs2) as t_inl_prs2,\n" +
                "COALESCE(t0.t_inl_tmp1,t1.t_inl_tmp1,t2.t_inl_tmp1,t3.t_inl_tmp1) as t_inl_tmp1,\n" +
                "COALESCE(t0.t_inl_tmp2,t1.t_inl_tmp2,t2.t_inl_tmp2,t3.t_inl_tmp2) as t_inl_tmp2,\n" +
                "COALESCE(t0.utc_hour,t1.utc_hour,t2.utc_hour,t3.utc_hour) as utc_hour,\n" +
                "COALESCE(t0.utc_min_sec,t1.utc_min_sec,t2.utc_min_sec,t3.utc_min_sec) as utc_min_sec,\n" +
                "COALESCE(t0.vor_frq1,t1.vor_frq1,t2.vor_frq1,t3.vor_frq1) as vor_frq1,\n" +
                "COALESCE(t0.vor_frq2,t1.vor_frq2,t2.vor_frq2,t3.vor_frq2) as vor_frq2,\n" +
                "COALESCE(t0.win_spdr,t1.win_spdr,t2.win_spdr,t3.win_spdr) as win_spdr,\n" +
                "COALESCE(t0.year_curr,t1.year_curr,t2.year_curr,t3.year_curr) as year_curr,\n" +
                "COALESCE(t0.year_med,t1.year_med,t2.year_med,t3.year_med) as year_med,\n" +
                "t0.flt_dt,t0.tail_num,t0.file_no\n" +
                "from\n" +
                "(select row_number() OVER(PARTITION BY file_no order by `time`,time_series asc ) as row_id_add,*\n" +
                "    from  czods.s_qara_320020_p25hz\n" +
                "    where  flt_dt='20241217'  and file_no='3659647'  ) t0\n" +
                "left join\n" +
                "(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+1 as row_id_add,*\n" +
                "    from  czods.s_qara_320020_p25hz\n" +
                "    where  flt_dt='20241217'  and file_no='3659647'  ) t1\n" +
                "on t0.flt_dt=t1.flt_dt and t0.file_no=t1.file_no and t0.tail_num=t1.tail_num and t0.row_id_add=t1.row_id_add\n" +
                "left join\n" +
                "(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+2 as row_id_add,*\n" +
                "    from  czods.s_qara_320020_p25hz\n" +
                "    where flt_dt='20241217'  and file_no='3659647'  ) t2\n" +
                "on t0.flt_dt=t2.flt_dt and t0.file_no=t2.file_no and t0.tail_num=t2.tail_num and t0.row_id_add=t2.row_id_add\n" +
                "    left join\n" +
                "(select (row_number() OVER(PARTITION BY file_no order by `time`,time_series asc )  )+3 as row_id_add,*\n" +
                "    from  czods.s_qara_320020_p25hz\n" +
                "    where  flt_dt='20241217'  and file_no='3659647'  ) t3\n" +
                "on t0.flt_dt=t3.flt_dt and t0.file_no=t3.file_no and t0.tail_num=t3.tail_num and t0.row_id_add=t3.row_id_add\n").print();
    }

    static void createParquetSource(TableEnvironment env) {
        env.executeSql("CREATE TABLE `default_catalog`.`default_database`.`mid_qara_320020_p25hz_fillup` (\n" +
                "  `file_no` VARCHAR(2147483647),\n" +
                "  `flt_dt` VARCHAR(2147483647),\n" +
                "  `tail_num` VARCHAR(2147483647),\n" +
                "  `time` INT,\n" +
                "  `time_series` INT,\n" +
                "  `acms_fp` INT,\n" +
                "  `acv_b_on2` INT,\n" +
                "  `ac_iden` INT,\n" +
                "  `ac_tail123` VARCHAR(2147483647),\n" +
                "  `ac_tail4` VARCHAR(2147483647),\n" +
                "  `ac_tail456` VARCHAR(2147483647),\n" +
                "  `ac_tail7` VARCHAR(2147483647),\n" +
                "  `ac_typ` INT,\n" +
                "  `ail_lh_avl_b` INT,\n" +
                "  `ail_lh_avl_g` INT,\n" +
                "  `ail_rh_avl_b` INT,\n" +
                "  `ail_rh_avl_g` INT,\n" +
                "  `airline_id` VARCHAR(2147483647),\n" +
                "  `airl_idt` VARCHAR(2147483647),\n" +
                "  `air_ground` FLOAT,\n" +
                "  `aiw_rw` INT,\n" +
                "  `alt_cpt` FLOAT,\n" +
                "  `alt_fo` FLOAT,\n" +
                "  `alt_swc` INT,\n" +
                "  `apr_ctl` INT,\n" +
                "  `apubld_vv_op` INT,\n" +
                "  `aspd_ctl` INT,\n" +
                "  `aud_tr_fail` INT,\n" +
                "  `bld_vlv1` INT,\n" +
                "  `bld_vlv2` INT,\n" +
                "  `bp1` FLOAT,\n" +
                "  `bp2` FLOAT,\n" +
                "  `cent_curr` INT,\n" +
                "  `city_from` VARCHAR(2147483647),\n" +
                "  `city_from_r` VARCHAR(2147483647),\n" +
                "  `city_to_r` VARCHAR(2147483647),\n" +
                "  `ck_conf_scl_mx` FLOAT,\n" +
                "  `ck_egt_scl_max` FLOAT,\n" +
                "  `ck_epr_scl_max` FLOAT,\n" +
                "  `ck_ins_eng_mod` INT,\n" +
                "  `ck_itt_scl_max` FLOAT,\n" +
                "  `ck_ldg_nr` INT,\n" +
                "  `ck_n1_scl_max` FLOAT,\n" +
                "  `ck_tla_scl_max` FLOAT,\n" +
                "  `ck_tla_scl_min` FLOAT,\n" +
                "  `ck_torq_scl_mx` FLOAT,\n" +
                "  `cmc_tr_fail` INT,\n" +
                "  `dat_day` FLOAT,\n" +
                "  `dat_month` FLOAT,\n" +
                "  `day_curr` INT,\n" +
                "  `db_upd_cyc` INT,\n" +
                "  `db_upd_date` INT,\n" +
                "  `dc1_bus_on` INT,\n" +
                "  `dc2_bus_on` INT,\n" +
                "  `dec_height` FLOAT,\n" +
                "  `dfc` INT,\n" +
                "  `dits_fail` INT,\n" +
                "  `dmc_1_invald` INT,\n" +
                "  `dmc_2_invald` INT,\n" +
                "  `dmc_3_invald` INT,\n" +
                "  `dmc_3_xfr_ca` INT,\n" +
                "  `dmc_3_xfr_fo` INT,\n" +
                "  `dmc_mark_id` INT,\n" +
                "  `dme_dis1` FLOAT,\n" +
                "  `dme_dis2` FLOAT,\n" +
                "  `dme_frq1` FLOAT,\n" +
                "  `dme_frq2` FLOAT,\n" +
                "  `drift` FLOAT,\n" +
                "  `ecamdu_1_off` INT,\n" +
                "  `ecamdu_2_off` INT,\n" +
                "  `ecamnd_xfrca` INT,\n" +
                "  `ecamnd_xfrfo` INT,\n" +
                "  `ecam_sel_mat` INT,\n" +
                "  `ecu_eec_1_cb` INT,\n" +
                "  `ecu_eec_2_cb` INT,\n" +
                "  `egp1` FLOAT,\n" +
                "  `egp2` FLOAT,\n" +
                "  `elac1_pt_flt` INT,\n" +
                "  `elac1_ro_flt` INT,\n" +
                "  `elac2_pt_flt` INT,\n" +
                "  `elac2_rl_flt` INT,\n" +
                "  `elac_1_fault` INT,\n" +
                "  `elac_2_fault` INT,\n" +
                "  `eng1_pt25` FLOAT,\n" +
                "  `eng2_pt25` FLOAT,\n" +
                "  `epr_max` FLOAT,\n" +
                "  `ext_tmp_eng1` FLOAT,\n" +
                "  `ext_tmp_eng2` FLOAT,\n" +
                "  `fdiu_fail` INT,\n" +
                "  `fdiu_prg_id` INT,\n" +
                "  `fdr_fail` INT,\n" +
                "  `fdr_pb_fail` INT,\n" +
                "  `flap_faul` INT,\n" +
                "  `flap_lever_c` FLOAT,\n" +
                "  `flare` FLOAT,\n" +
                "  `fleet_idt` INT,\n" +
                "  `flight_phas0` INT,\n" +
                "  `fltnum` FLOAT,\n" +
                "  `fwc_valid` INT,\n" +
                "  `gw` FLOAT,\n" +
                "  `head_selon` INT,\n" +
                "  `hpv_nfc_1` INT,\n" +
                "  `hpv_nfc_2` INT,\n" +
                "  `ident_eng1` FLOAT,\n" +
                "  `ident_eng2` FLOAT,\n" +
                "  `ils_frq1` FLOAT,\n" +
                "  `ils_frq2` FLOAT,\n" +
                "  `isov1` INT,\n" +
                "  `isov2` INT,\n" +
                "  `ivv_c` FLOAT,\n" +
                "  `landing_roll` FLOAT,\n" +
                "  `latp` FLOAT,\n" +
                "  `lat_ck_fail` INT,\n" +
                "  `lat_ck_n_r` INT,\n" +
                "  `ldg_seldw` INT,\n" +
                "  `ldg_selup` INT,\n" +
                "  `ldg_sel_up` FLOAT,\n" +
                "  `long_ck_fail` INT,\n" +
                "  `long_ck_n_r` INT,\n" +
                "  `lonp` FLOAT,\n" +
                "  `mach_selon` INT,\n" +
                "  `mls_select` INT,\n" +
                "  `mode_mmr1` INT,\n" +
                "  `mode_mmr2` INT,\n" +
                "  `mode_nd_capt` INT,\n" +
                "  `mode_nd_fo` INT,\n" +
                "  `month_curr` INT,\n" +
                "  `n1_epr_tgt1` INT,\n" +
                "  `nd_ca_an_off` INT,\n" +
                "  `nd_fo_an_off` INT,\n" +
                "  `nm_range_ca` INT,\n" +
                "  `nm_range_fo` INT,\n" +
                "  `norm_ck_fail` INT,\n" +
                "  `norm_ck_n_r` INT,\n" +
                "  `no_data_eec1` INT,\n" +
                "  `no_data_eec2` INT,\n" +
                "  `oil_prs1` FLOAT,\n" +
                "  `oil_prs2` FLOAT,\n" +
                "  `oil_tmp1` FLOAT,\n" +
                "  `oil_tmp2` FLOAT,\n" +
                "  `oiq1` FLOAT,\n" +
                "  `oiq2` FLOAT,\n" +
                "  `opv_nfo_1` INT,\n" +
                "  `opv_nfo_2` INT,\n" +
                "  `pfdnd_xfr_ca` INT,\n" +
                "  `pfdnd_xfr_fo` INT,\n" +
                "  `pfd_ca_anoff` INT,\n" +
                "  `pfd_fo_anoff` INT,\n" +
                "  `pitch_altr1` INT,\n" +
                "  `pitch_altr2` INT,\n" +
                "  `pitch_dir_lw` INT,\n" +
                "  `pitch_nor` INT,\n" +
                "  `playbk_fail` INT,\n" +
                "  `playbk_inact` INT,\n" +
                "  `prv_nfc_1` INT,\n" +
                "  `prv_nfc_2` INT,\n" +
                "  `qar_fail` INT,\n" +
                "  `qar_tapelow` INT,\n" +
                "  `sdac1_valid` INT,\n" +
                "  `sdac2_valid` INT,\n" +
                "  `sec_1_fault` INT,\n" +
                "  `sec_2_fault` INT,\n" +
                "  `sec_3_fault` INT,\n" +
                "  `slat_faul` INT,\n" +
                "  `spd_brk` INT,\n" +
                "  `stab_bld_pos1` FLOAT,\n" +
                "  `stab_bld_pos2` FLOAT,\n" +
                "  `start_vlv1` INT,\n" +
                "  `start_vlv2` INT,\n" +
                "  `sta_van_eng1` FLOAT,\n" +
                "  `sta_van_eng2` FLOAT,\n" +
                "  `tcas_sens_1` INT,\n" +
                "  `tcas_sens_2` INT,\n" +
                "  `tcas_sens_3` INT,\n" +
                "  `tla1_c` FLOAT,\n" +
                "  `tla2_c` FLOAT,\n" +
                "  `touch_and_go` FLOAT,\n" +
                "  `trb1_cool` FLOAT,\n" +
                "  `trb2_cool` FLOAT,\n" +
                "  `tt25_l` FLOAT,\n" +
                "  `tt25_r` FLOAT,\n" +
                "  `t_inl_prs1` FLOAT,\n" +
                "  `t_inl_prs2` FLOAT,\n" +
                "  `t_inl_tmp1` FLOAT,\n" +
                "  `t_inl_tmp2` FLOAT,\n" +
                "  `utc_hour` FLOAT,\n" +
                "  `utc_min_sec` INT,\n" +
                "  `vor_frq1` FLOAT,\n" +
                "  `vor_frq2` FLOAT,\n" +
                "  `win_spdr` FLOAT,\n" +
                "  `year_curr` INT,\n" +
                "  `year_med` FLOAT\n" +
                ") " +
//                "PARTITIONED BY (`flt_dt`, `tail_num`, `file_no`)\n" +
                "WITH (\n" +
                "  'connector' = 'filesystem',\n" +
                "  'path' = 'file:///Users/ceng/Downloads/part-UMOHzFhxsSwFAocI_0000.parquet',\n" +
                "  'format' = 'parquet'\n" +
                ");\n" +
                "\n");
    }

    static void createLakeSoulSinkTable(TableEnvironment env) {
        env.executeSql("CREATE TABLE qara_mid.mid_qara_320020_p25hz_fillup (\n" +
                "  `time` INT,\n" +
                "  `time_series` INT,\n" +
                "  `acms_fp` INT,\n" +
                "  `acv_b_on2` INT,\n" +
                "  `ac_iden` INT,\n" +
                "  `ac_tail123` VARCHAR(2147483647),\n" +
                "  `ac_tail4` VARCHAR(2147483647),\n" +
                "  `ac_tail456` VARCHAR(2147483647),\n" +
                "  `ac_tail7` VARCHAR(2147483647),\n" +
                "  `ac_typ` INT,\n" +
                "  `ail_lh_avl_b` INT,\n" +
                "  `ail_lh_avl_g` INT,\n" +
                "  `ail_rh_avl_b` INT,\n" +
                "  `ail_rh_avl_g` INT,\n" +
                "  `airline_id` VARCHAR(2147483647),\n" +
                "  `airl_idt` VARCHAR(2147483647),\n" +
                "  `air_ground` FLOAT,\n" +
                "  `aiw_rw` INT,\n" +
                "  `alt_cpt` FLOAT,\n" +
                "  `alt_fo` FLOAT,\n" +
                "  `alt_swc` INT,\n" +
                "  `apr_ctl` INT,\n" +
                "  `apubld_vv_op` INT,\n" +
                "  `aspd_ctl` INT,\n" +
                "  `aud_tr_fail` INT,\n" +
                "  `bld_vlv1` INT,\n" +
                "  `bld_vlv2` INT,\n" +
                "  `bp1` FLOAT,\n" +
                "  `bp2` FLOAT,\n" +
                "  `cent_curr` INT,\n" +
                "  `city_from` VARCHAR(2147483647),\n" +
                "  `city_from_r` VARCHAR(2147483647),\n" +
                "  `city_to_r` VARCHAR(2147483647),\n" +
                "  `ck_conf_scl_mx` FLOAT,\n" +
                "  `ck_egt_scl_max` FLOAT,\n" +
                "  `ck_epr_scl_max` FLOAT,\n" +
                "  `ck_ins_eng_mod` INT,\n" +
                "  `ck_itt_scl_max` FLOAT,\n" +
                "  `ck_ldg_nr` INT,\n" +
                "  `ck_n1_scl_max` FLOAT,\n" +
                "  `ck_tla_scl_max` FLOAT,\n" +
                "  `ck_tla_scl_min` FLOAT,\n" +
                "  `ck_torq_scl_mx` FLOAT,\n" +
                "  `cmc_tr_fail` INT,\n" +
                "  `dat_day` FLOAT,\n" +
                "  `dat_month` FLOAT,\n" +
                "  `day_curr` INT,\n" +
                "  `db_upd_cyc` INT,\n" +
                "  `db_upd_date` INT,\n" +
                "  `dc1_bus_on` INT,\n" +
                "  `dc2_bus_on` INT,\n" +
                "  `dec_height` FLOAT,\n" +
                "  `dfc` INT,\n" +
                "  `dits_fail` INT,\n" +
                "  `dmc_1_invald` INT,\n" +
                "  `dmc_2_invald` INT,\n" +
                "  `dmc_3_invald` INT,\n" +
                "  `dmc_3_xfr_ca` INT,\n" +
                "  `dmc_3_xfr_fo` INT,\n" +
                "  `dmc_mark_id` INT,\n" +
                "  `dme_dis1` FLOAT,\n" +
                "  `dme_dis2` FLOAT,\n" +
                "  `dme_frq1` FLOAT,\n" +
                "  `dme_frq2` FLOAT,\n" +
                "  `drift` FLOAT,\n" +
                "  `ecamdu_1_off` INT,\n" +
                "  `ecamdu_2_off` INT,\n" +
                "  `ecamnd_xfrca` INT,\n" +
                "  `ecamnd_xfrfo` INT,\n" +
                "  `ecam_sel_mat` INT,\n" +
                "  `ecu_eec_1_cb` INT,\n" +
                "  `ecu_eec_2_cb` INT,\n" +
                "  `egp1` FLOAT,\n" +
                "  `egp2` FLOAT,\n" +
                "  `elac1_pt_flt` INT,\n" +
                "  `elac1_ro_flt` INT,\n" +
                "  `elac2_pt_flt` INT,\n" +
                "  `elac2_rl_flt` INT,\n" +
                "  `elac_1_fault` INT,\n" +
                "  `elac_2_fault` INT,\n" +
                "  `eng1_pt25` FLOAT,\n" +
                "  `eng2_pt25` FLOAT,\n" +
                "  `epr_max` FLOAT,\n" +
                "  `ext_tmp_eng1` FLOAT,\n" +
                "  `ext_tmp_eng2` FLOAT,\n" +
                "  `fdiu_fail` INT,\n" +
                "  `fdiu_prg_id` INT,\n" +
                "  `fdr_fail` INT,\n" +
                "  `fdr_pb_fail` INT,\n" +
                "  `flap_faul` INT,\n" +
                "  `flap_lever_c` FLOAT,\n" +
                "  `flare` FLOAT,\n" +
                "  `fleet_idt` INT,\n" +
                "  `flight_phas0` INT,\n" +
                "  `fltnum` FLOAT,\n" +
                "  `fwc_valid` INT,\n" +
                "  `gw` FLOAT,\n" +
                "  `head_selon` INT,\n" +
                "  `hpv_nfc_1` INT,\n" +
                "  `hpv_nfc_2` INT,\n" +
                "  `ident_eng1` FLOAT,\n" +
                "  `ident_eng2` FLOAT,\n" +
                "  `ils_frq1` FLOAT,\n" +
                "  `ils_frq2` FLOAT,\n" +
                "  `isov1` INT,\n" +
                "  `isov2` INT,\n" +
                "  `ivv_c` FLOAT,\n" +
                "  `landing_roll` FLOAT,\n" +
                "  `latp` FLOAT,\n" +
                "  `lat_ck_fail` INT,\n" +
                "  `lat_ck_n_r` INT,\n" +
                "  `ldg_seldw` INT,\n" +
                "  `ldg_selup` INT,\n" +
                "  `ldg_sel_up` FLOAT,\n" +
                "  `long_ck_fail` INT,\n" +
                "  `long_ck_n_r` INT,\n" +
                "  `lonp` FLOAT,\n" +
                "  `mach_selon` INT,\n" +
                "  `mls_select` INT,\n" +
                "  `mode_mmr1` INT,\n" +
                "  `mode_mmr2` INT,\n" +
                "  `mode_nd_capt` INT,\n" +
                "  `mode_nd_fo` INT,\n" +
                "  `month_curr` INT,\n" +
                "  `n1_epr_tgt1` INT,\n" +
                "  `nd_ca_an_off` INT,\n" +
                "  `nd_fo_an_off` INT,\n" +
                "  `nm_range_ca` INT,\n" +
                "  `nm_range_fo` INT,\n" +
                "  `norm_ck_fail` INT,\n" +
                "  `norm_ck_n_r` INT,\n" +
                "  `no_data_eec1` INT,\n" +
                "  `no_data_eec2` INT,\n" +
                "  `oil_prs1` FLOAT,\n" +
                "  `oil_prs2` FLOAT,\n" +
                "  `oil_tmp1` FLOAT,\n" +
                "  `oil_tmp2` FLOAT,\n" +
                "  `oiq1` FLOAT,\n" +
                "  `oiq2` FLOAT,\n" +
                "  `opv_nfo_1` INT,\n" +
                "  `opv_nfo_2` INT,\n" +
                "  `pfdnd_xfr_ca` INT,\n" +
                "  `pfdnd_xfr_fo` INT,\n" +
                "  `pfd_ca_anoff` INT,\n" +
                "  `pfd_fo_anoff` INT,\n" +
                "  `pitch_altr1` INT,\n" +
                "  `pitch_altr2` INT,\n" +
                "  `pitch_dir_lw` INT,\n" +
                "  `pitch_nor` INT,\n" +
                "  `playbk_fail` INT,\n" +
                "  `playbk_inact` INT,\n" +
                "  `prv_nfc_1` INT,\n" +
                "  `prv_nfc_2` INT,\n" +
                "  `qar_fail` INT,\n" +
                "  `qar_tapelow` INT,\n" +
                "  `sdac1_valid` INT,\n" +
                "  `sdac2_valid` INT,\n" +
                "  `sec_1_fault` INT,\n" +
                "  `sec_2_fault` INT,\n" +
                "  `sec_3_fault` INT,\n" +
                "  `slat_faul` INT,\n" +
                "  `spd_brk` INT,\n" +
                "  `stab_bld_pos1` FLOAT,\n" +
                "  `stab_bld_pos2` FLOAT,\n" +
                "  `start_vlv1` INT,\n" +
                "  `start_vlv2` INT,\n" +
                "  `sta_van_eng1` FLOAT,\n" +
                "  `sta_van_eng2` FLOAT,\n" +
                "  `tcas_sens_1` INT,\n" +
                "  `tcas_sens_2` INT,\n" +
                "  `tcas_sens_3` INT,\n" +
                "  `tla1_c` FLOAT,\n" +
                "  `tla2_c` FLOAT,\n" +
                "  `touch_and_go` FLOAT,\n" +
                "  `trb1_cool` FLOAT,\n" +
                "  `trb2_cool` FLOAT,\n" +
                "  `tt25_l` FLOAT,\n" +
                "  `tt25_r` FLOAT,\n" +
                "  `t_inl_prs1` FLOAT,\n" +
                "  `t_inl_prs2` FLOAT,\n" +
                "  `t_inl_tmp1` FLOAT,\n" +
                "  `t_inl_tmp2` FLOAT,\n" +
                "  `utc_hour` FLOAT,\n" +
                "  `utc_min_sec` INT,\n" +
                "  `vor_frq1` FLOAT,\n" +
                "  `vor_frq2` FLOAT,\n" +
                "  `win_spdr` FLOAT,\n" +
                "  `year_curr` INT,\n" +
                "  `year_med` FLOAT,\n" +
                "  `flt_dt` VARCHAR(2147483647),\n" +
                "  `tail_num` VARCHAR(2147483647),\n" +
                "  `file_no` VARCHAR(2147483647)\n" +
                ") PARTITIONED BY (`flt_dt`, `tail_num`, `file_no`)\n" +
                "WITH (\n" +
                "  'hashBucketNum' = '-1',\n" +
                "  'path' = '" + getTempDirUri("/mid_qara_320020_p25hz_fillup") + "',\n" +
                "  'connector' = 'lakesoul',\n" +
                "  'last_schema_change_time' = '1730721069211'\n" +
                ");\n");

        env.executeSql("CREATE TABLE `czods`.`s_qara_320020_p25hz` (\n" +
                "  `file_no` VARCHAR(2147483647),\n" +
                "  `flt_dt` VARCHAR(2147483647),\n" +
                "  `tail_num` VARCHAR(2147483647),\n" +
                "  `time` INT,\n" +
                "  `time_series` INT,\n" +
                "  `acms_fp` INT,\n" +
                "  `acv_b_on2` INT,\n" +
                "  `ac_iden` INT,\n" +
                "  `ac_tail123` VARCHAR(2147483647),\n" +
                "  `ac_tail4` VARCHAR(2147483647),\n" +
                "  `ac_tail456` VARCHAR(2147483647),\n" +
                "  `ac_tail7` VARCHAR(2147483647),\n" +
                "  `ac_typ` INT,\n" +
                "  `ail_lh_avl_b` INT,\n" +
                "  `ail_lh_avl_g` INT,\n" +
                "  `ail_rh_avl_b` INT,\n" +
                "  `ail_rh_avl_g` INT,\n" +
                "  `airline_id` VARCHAR(2147483647),\n" +
                "  `airl_idt` VARCHAR(2147483647),\n" +
                "  `air_ground` FLOAT,\n" +
                "  `aiw_rw` INT,\n" +
                "  `alt_cpt` FLOAT,\n" +
                "  `alt_fo` FLOAT,\n" +
                "  `alt_swc` INT,\n" +
                "  `apr_ctl` INT,\n" +
                "  `apubld_vv_op` INT,\n" +
                "  `aspd_ctl` INT,\n" +
                "  `aud_tr_fail` INT,\n" +
                "  `bld_vlv1` INT,\n" +
                "  `bld_vlv2` INT,\n" +
                "  `bp1` FLOAT,\n" +
                "  `bp2` FLOAT,\n" +
                "  `cent_curr` INT,\n" +
                "  `city_from` VARCHAR(2147483647),\n" +
                "  `city_from_r` VARCHAR(2147483647),\n" +
                "  `city_to_r` VARCHAR(2147483647),\n" +
                "  `ck_conf_scl_mx` FLOAT,\n" +
                "  `ck_egt_scl_max` FLOAT,\n" +
                "  `ck_epr_scl_max` FLOAT,\n" +
                "  `ck_ins_eng_mod` INT,\n" +
                "  `ck_itt_scl_max` FLOAT,\n" +
                "  `ck_ldg_nr` INT,\n" +
                "  `ck_n1_scl_max` FLOAT,\n" +
                "  `ck_tla_scl_max` FLOAT,\n" +
                "  `ck_tla_scl_min` FLOAT,\n" +
                "  `ck_torq_scl_mx` FLOAT,\n" +
                "  `cmc_tr_fail` INT,\n" +
                "  `dat_day` FLOAT,\n" +
                "  `dat_month` FLOAT,\n" +
                "  `day_curr` INT,\n" +
                "  `db_upd_cyc` INT,\n" +
                "  `db_upd_date` INT,\n" +
                "  `dc1_bus_on` INT,\n" +
                "  `dc2_bus_on` INT,\n" +
                "  `dec_height` FLOAT,\n" +
                "  `dfc` INT,\n" +
                "  `dits_fail` INT,\n" +
                "  `dmc_1_invald` INT,\n" +
                "  `dmc_2_invald` INT,\n" +
                "  `dmc_3_invald` INT,\n" +
                "  `dmc_3_xfr_ca` INT,\n" +
                "  `dmc_3_xfr_fo` INT,\n" +
                "  `dmc_mark_id` INT,\n" +
                "  `dme_dis1` FLOAT,\n" +
                "  `dme_dis2` FLOAT,\n" +
                "  `dme_frq1` FLOAT,\n" +
                "  `dme_frq2` FLOAT,\n" +
                "  `drift` FLOAT,\n" +
                "  `ecamdu_1_off` INT,\n" +
                "  `ecamdu_2_off` INT,\n" +
                "  `ecamnd_xfrca` INT,\n" +
                "  `ecamnd_xfrfo` INT,\n" +
                "  `ecam_sel_mat` INT,\n" +
                "  `ecu_eec_1_cb` INT,\n" +
                "  `ecu_eec_2_cb` INT,\n" +
                "  `egp1` FLOAT,\n" +
                "  `egp2` FLOAT,\n" +
                "  `elac1_pt_flt` INT,\n" +
                "  `elac1_ro_flt` INT,\n" +
                "  `elac2_pt_flt` INT,\n" +
                "  `elac2_rl_flt` INT,\n" +
                "  `elac_1_fault` INT,\n" +
                "  `elac_2_fault` INT,\n" +
                "  `eng1_pt25` FLOAT,\n" +
                "  `eng2_pt25` FLOAT,\n" +
                "  `epr_max` FLOAT,\n" +
                "  `ext_tmp_eng1` FLOAT,\n" +
                "  `ext_tmp_eng2` FLOAT,\n" +
                "  `fdiu_fail` INT,\n" +
                "  `fdiu_prg_id` INT,\n" +
                "  `fdr_fail` INT,\n" +
                "  `fdr_pb_fail` INT,\n" +
                "  `flap_faul` INT,\n" +
                "  `flap_lever_c` FLOAT,\n" +
                "  `flare` FLOAT,\n" +
                "  `fleet_idt` INT,\n" +
                "  `flight_phas0` INT,\n" +
                "  `fltnum` FLOAT,\n" +
                "  `fwc_valid` INT,\n" +
                "  `gw` FLOAT,\n" +
                "  `head_selon` INT,\n" +
                "  `hpv_nfc_1` INT,\n" +
                "  `hpv_nfc_2` INT,\n" +
                "  `ident_eng1` FLOAT,\n" +
                "  `ident_eng2` FLOAT,\n" +
                "  `ils_frq1` FLOAT,\n" +
                "  `ils_frq2` FLOAT,\n" +
                "  `isov1` INT,\n" +
                "  `isov2` INT,\n" +
                "  `ivv_c` FLOAT,\n" +
                "  `landing_roll` FLOAT,\n" +
                "  `latp` FLOAT,\n" +
                "  `lat_ck_fail` INT,\n" +
                "  `lat_ck_n_r` INT,\n" +
                "  `ldg_seldw` INT,\n" +
                "  `ldg_selup` INT,\n" +
                "  `ldg_sel_up` FLOAT,\n" +
                "  `long_ck_fail` INT,\n" +
                "  `long_ck_n_r` INT,\n" +
                "  `lonp` FLOAT,\n" +
                "  `mach_selon` INT,\n" +
                "  `mls_select` INT,\n" +
                "  `mode_mmr1` INT,\n" +
                "  `mode_mmr2` INT,\n" +
                "  `mode_nd_capt` INT,\n" +
                "  `mode_nd_fo` INT,\n" +
                "  `month_curr` INT,\n" +
                "  `n1_epr_tgt1` INT,\n" +
                "  `nd_ca_an_off` INT,\n" +
                "  `nd_fo_an_off` INT,\n" +
                "  `nm_range_ca` INT,\n" +
                "  `nm_range_fo` INT,\n" +
                "  `norm_ck_fail` INT,\n" +
                "  `norm_ck_n_r` INT,\n" +
                "  `no_data_eec1` INT,\n" +
                "  `no_data_eec2` INT,\n" +
                "  `oil_prs1` FLOAT,\n" +
                "  `oil_prs2` FLOAT,\n" +
                "  `oil_tmp1` FLOAT,\n" +
                "  `oil_tmp2` FLOAT,\n" +
                "  `oiq1` FLOAT,\n" +
                "  `oiq2` FLOAT,\n" +
                "  `opv_nfo_1` INT,\n" +
                "  `opv_nfo_2` INT,\n" +
                "  `pfdnd_xfr_ca` INT,\n" +
                "  `pfdnd_xfr_fo` INT,\n" +
                "  `pfd_ca_anoff` INT,\n" +
                "  `pfd_fo_anoff` INT,\n" +
                "  `pitch_altr1` INT,\n" +
                "  `pitch_altr2` INT,\n" +
                "  `pitch_dir_lw` INT,\n" +
                "  `pitch_nor` INT,\n" +
                "  `playbk_fail` INT,\n" +
                "  `playbk_inact` INT,\n" +
                "  `prv_nfc_1` INT,\n" +
                "  `prv_nfc_2` INT,\n" +
                "  `qar_fail` INT,\n" +
                "  `qar_tapelow` INT,\n" +
                "  `sdac1_valid` INT,\n" +
                "  `sdac2_valid` INT,\n" +
                "  `sec_1_fault` INT,\n" +
                "  `sec_2_fault` INT,\n" +
                "  `sec_3_fault` INT,\n" +
                "  `slat_faul` INT,\n" +
                "  `spd_brk` INT,\n" +
                "  `stab_bld_pos1` FLOAT,\n" +
                "  `stab_bld_pos2` FLOAT,\n" +
                "  `start_vlv1` INT,\n" +
                "  `start_vlv2` INT,\n" +
                "  `sta_van_eng1` FLOAT,\n" +
                "  `sta_van_eng2` FLOAT,\n" +
                "  `tcas_sens_1` INT,\n" +
                "  `tcas_sens_2` INT,\n" +
                "  `tcas_sens_3` INT,\n" +
                "  `tla1_c` FLOAT,\n" +
                "  `tla2_c` FLOAT,\n" +
                "  `touch_and_go` FLOAT,\n" +
                "  `trb1_cool` FLOAT,\n" +
                "  `trb2_cool` FLOAT,\n" +
                "  `tt25_l` FLOAT,\n" +
                "  `tt25_r` FLOAT,\n" +
                "  `t_inl_prs1` FLOAT,\n" +
                "  `t_inl_prs2` FLOAT,\n" +
                "  `t_inl_tmp1` FLOAT,\n" +
                "  `t_inl_tmp2` FLOAT,\n" +
                "  `utc_hour` FLOAT,\n" +
                "  `utc_min_sec` INT,\n" +
                "  `vor_frq1` FLOAT,\n" +
                "  `vor_frq2` FLOAT,\n" +
                "  `win_spdr` FLOAT,\n" +
                "  `year_curr` INT,\n" +
                "  `year_med` FLOAT\n" +
                ") PARTITIONED BY (`flt_dt`, `tail_num`, `file_no`)\n" +
                "WITH (\n" +
                "  'hashBucketNum' = '-1',\n" +
                "  'path' = '" + getTempDirUri("/s_qara_320020_p25hz") + "',\n" +
                "  'connector' = 'lakesoul',\n" +
                "  'last_schema_change_time' = '1730721069211'\n" +
                ");\n" +
                "\n");
    }
}
