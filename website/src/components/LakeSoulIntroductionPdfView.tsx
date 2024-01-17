import { Viewer, Worker } from '@react-pdf-viewer/core';
import { defaultLayoutPlugin } from '@react-pdf-viewer/default-layout';

import '@react-pdf-viewer/core/lib/styles/index.css';
import '@react-pdf-viewer/default-layout/lib/styles/index.css';

import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

const LakeSoulIntroPdfViewer = () => {
    const defaultLayoutPluginInstance = defaultLayoutPlugin();

    const { siteConfig, i18n } = useDocusaurusContext();
    let pdfUrl = '/lakesoul-introduction.pdf';
    if (i18n.currentLocale === 'en') {
        pdfUrl = '/lakesoul-introduction-en.pdf';
    }

    return (
        <Worker workerUrl="https://unpkg.com/pdfjs-dist@3.11.174/build/pdf.worker.js">
            <div
                style={{
                    height: '750px',
                    width: '900px',
                    marginLeft: 'auto',
                    marginRight: 'auto',
                }}
            >
                <Viewer fileUrl={pdfUrl} plugins={[defaultLayoutPluginInstance]} />
            </div>
        </Worker>
    );
};

export default LakeSoulIntroPdfViewer;
