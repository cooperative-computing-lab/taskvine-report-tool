import { BaseModule } from './base.js?v=5.0';

export class FileCreatedSizeModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);
        this.setBottomScaleType('linear');
        this.setLeftScaleType('linear');
    }

    plot() {
        if (!this.data) return;

        this.plotPath(this.data.points, {
            className: 'file-created-size-path',
        });
    }
} 