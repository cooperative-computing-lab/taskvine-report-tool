import { BaseModule } from './base.js';

export class TaskCompletionPercentilesModule extends BaseModule {
    constructor(id, title, api_url) {
        super(id, title, api_url);
        this.setBottomScaleType('band');
        this.setLeftScaleType('linear');
    }

    plot() {
        if (!this.data) return;

        const xWidth = this.bottomScale.bandwidth() * 0.8;
        const yFormatter = eval(this.data['y_tick_formatter']);

        this.data['points'].forEach(([percentile, time]) => {
            const innerHTML = `Percentile: ${percentile}%<br>Time: ${yFormatter(time)}`;
            this.plotVerticalRect(percentile, xWidth, time, 'steelblue', 1, innerHTML);
        });
    }
} 