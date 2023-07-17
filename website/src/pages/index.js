// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

import React from 'react';
import Layout from '@theme/Layout';
import Translate, { translate } from '@docusaurus/Translate'
import styles from './index.module.css';
import GitHubButton from 'react-github-btn';
import LakeSoulLogo from '@site/static/img/LakeSoul_Horizontal_White.svg'
const cardList = [
  {
      title: <Translate id="homepage.cardList1Title">Centralized Metadata Service </Translate>,
      description: <Translate id="homepage.cardList1Description">LakeSoul uses PostgreSQL to store metadata, improving metadata's scalability and allowing higer concurrency with guaranteed consistency.</Translate>,
  },
  {
      title: <Translate id="homepage.cardList2Title">Concurrent Writes and ACID</Translate>,
      description: <Translate id="homepage.cardList2Description">Concurrency control through PG, together with auto conflict resolving, providing a high write concurrency ability.</Translate>,
  },
  {
      title: <Translate id="homepage.cardList3Title">Upsert and Incremental Read</Translate>,
      description: <Translate id="homepage.cardList3Description">LakeSoul supports concurrent upsert and incremental read from tables as changelog format</Translate>,
  },
  {
      title: <Translate id="homepage.cardList4Title">Real-time Lakehouse</Translate>,
      description: <Translate id="homepage.cardList4Description">LakeSoul helps building large scale real-time lakehouse in both SQL and Python with incremental compute pipeline, on your existing Hadoop or K8s cluster</Translate>,
  },
  {
      title: <Translate id="homepage.cardList5Title">BI & AI Enabler</Translate>,
      description: <Translate id="homepage.cardList5Description">Use SQL to analyze your data at scale, at ease. Native support for Python reader allows accessing tables from any data science and AI tools</Translate>,
  }
]
function HomepageHeader(){
  function toDocument(href) {
    location.href = href
  }
  const buttonList = [
    {
        link: 'https://github.com/lakesoul-io/LakeSoul',
        value: 'Github'
    },
    {
        link: '/docs/intro',
        value: <Translate id="homepage.documentBtn">Documentation</Translate>
    },
  ]
  return (
    <div>
      <div className={styles.header}>
        <LakeSoulLogo className={styles.lakeLogo}/>    
        <p className={styles.ifaiDescription}><Translate id="homepage.ifaiDescription">Linux Foundation AI & Data</Translate></p>   
        <div className={styles.hr}></div> 
        <p className={styles.description} ><Translate id="homepage.description2">Building end-to-end realtime lakehouse with transactional concurrent upsert, incremental pipeline and SQL for your BI & AI applications</Translate></p>
        <div className={styles.btnBox}>
            {buttonList.map((btnItem, btnIndex) => {
              return (
                  <a href={btnItem.link} key={btnIndex}><button className={[styles.btn].join(' ')}>{btnItem.value}</button></a>
              )
            })}
        </div>
        <GitHubButton href='https://github.com/lakesoul-io/LakeSoul' data-size="large" data-show-count="true" style={{display:'inline-block'}}>Star</GitHubButton>
      </div>
    </div>
  )
}

export default function Home() {
  // const {siteConfig} = useDocusaurusContext();
  return (
    <Layout>

      <HomepageHeader/>

      <div className={styles.bcgBox}>
        <div>
          <p className={styles.sectionTitle}><Translate id="homepage.conceptionListTitle">Major Features of LakeSoul</Translate></p>
          {/* <p className={styles.descriptionText}><Translate id="homepage.conceptionListDescription">Traditional data architecture is faced with the untimely response, high cost, inability to unify real-time data, batch data, and difficulty scaling. LakeSoul provides a perfect lake warehouse storage to solve the above problems. It offers high concurrency, high throughput, read and write capabilities and complete warehouse management capabilities on the cloud and provides it to various computing engines in a general way. </Translate></p> */}
        </div>
        
        <div className={styles.cardBox}>
          {cardList.map((item, index) => {
            return (
              <div className={styles.card} key={index}>
                <p className={styles.cardTitle}>{item.title}</p>
                <p className={styles.sectionDescription}>{item.description}</p>
              </div>)
          })}
        </div>
      </div>

      <div className={styles.bcgBox} style={{backgroundColor:'#F4FAFF'}}>
        <p className={styles.sectionTitle}><Translate id="homepage.sectionTitle">Architectural Design to Unified Stream and Batch for Both Storage and Computing</Translate></p>
        <img src={require('@site/static/img/lakeSoulModel.png').default} className={styles.modelImg}></img>
      </div>

      <p className={styles.sectionTitle2}><Translate id="homepage.sectionTitle2">Designed for Both BI and AI to Maximize the Value of Your Data</Translate></p>
      <div className={[styles.bcgBox,styles.centerBox].join(' ')}>
        <div className={styles.sectionChildBox}>
          <img src={require('@site/static/img/sql.png').default} className={styles.sectionImg} style={{width:'500px',height:'230px',marginLeft:'0'}}></img>
          <div className={styles.textBox}>
            <p className={styles.cardTitle}><Translate id="homepage.sectionWords">Real-time Data Ingestion</Translate></p>
            <p className={styles.sectionDescription}><Translate id="homepage.sectionWords2">Data from various sources, including Kafka, Debezium and Flink CDC can be easily ingested into LakeSoul in real-time with high concurrency and throughput</Translate></p>
          </div>
        </div>
      </div>
      <div className={[styles.bcgBox,styles.centerBox].join(' ')} style={{backgroundColor:'#F4FAFF'}}>
        <div className={styles.sectionChildBox}>
          <div className={styles.textBox}>
            <p className={styles.cardTitle}><Translate id="homepage.sectionWords3">Real-time Data Analytics</Translate></p>
            <p className={styles.sectionDescription}><Translate id="homepage.sectionWords4">LakeSoul allows reading its table as incremental stream in both Spark and Flink to build real-time data transformation pipeline and do analytics in SQL</Translate></p>
          </div>
          <img src={require('@site/static/img/frame.png').default} className={styles.sectionImg} style={{width:'450px',height:'330px',marginRight:'0'}}></img>
        </div>
      </div>
      <div className={[styles.bcgBox,styles.centerBox].join(' ')}>
        <div className={styles.sectionChildBox}>
          <img src={require('@site/static/img/ai.png').default} className={styles.sectionImg} style={{width:'500px',height:'350px',marginLeft:'0'}}></img>
          <div className={styles.textBox}>
            <p className={styles.cardTitle}><Translate id="homepage.sectionWords5">AI Applications</Translate></p>
            <p className={styles.sectionDescription}><Translate id="homepage.sectionWords6">LakeSoul natively supports writing multiple streams into one table with primary key, and enables building real-time tabular dataset for AI applications</Translate></p>
          </div>
        </div>
      </div>
    </Layout>
  );
}
