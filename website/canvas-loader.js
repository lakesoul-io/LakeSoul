module.exports = () => ({
    name: 'canvas-loader',
    configureWebpack() {
        return {
            // It's required by pdfjs-dist
            externals: [{
                canvas: 'canvas',
            }],
        };
    },
});