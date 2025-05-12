export class LogManager {
    constructor() {
        this.selectorId = 'log-selector';
        this.apiUrl = '/api/runtime-template-list';
        this.selector = document.getElementById(this.selectorId);
        this._logChangeCallbacks = [];
        this.lockKey = 'log-change-in-progress';
    }

    async init() {
        try {
            const response = await fetch(this.apiUrl);
            const logFolders = await response.json();

            this._populateSelector(logFolders);
            this.selector.addEventListener('change', () => this._handleUserLogChange());

            window.addEventListener('storage', (e) => {
                if (e.key === this.lockKey && e.newValue === null) {
                    this.selector.disabled = false;
                }
            });

            const saved = sessionStorage.getItem('selectedLogFolder');
            if (saved && logFolders.includes(saved)) {
                this.selector.value = saved;
                this.selector.dispatchEvent(new Event('change'));
            }
        } catch (err) {
            console.error('Failed to initialize log manager:', err);
        }
    }

    onChange(callback) {
        if (typeof callback === 'function') {
            this._logChangeCallbacks.push(callback);
        }
    }

    _getCurrentLogFolder() {
        return this.selector.value;
    }

    _populateSelector(folders) {
        this.selector.innerHTML = '';

        const defaultOption = document.createElement('option');
        defaultOption.value = '';
        defaultOption.textContent = '--- select log ---';
        defaultOption.selected = true;
        this.selector.appendChild(defaultOption);

        folders.forEach(folder => {
            const option = document.createElement('option');
            option.value = folder;
            option.textContent = folder;
            this.selector.appendChild(option);
        });
    }

    async _handleUserLogChange() {
        const folder = this._getCurrentLogFolder();
        if (!folder) return;

        this._acquireLock();
        this.selector.disabled = true;

        sessionStorage.setItem('selectedLogFolder', folder);

        try {
            document.querySelectorAll('.error-tip').forEach(tip => {
                tip.style.visibility = 'hidden';
            });

            const res = await fetch(`/api/change-runtime-template?runtime_template=${folder}`);
            const result = await res.json();

            if (result.success) {
                this._logChangeCallbacks.forEach(callback => {
                    try {
                        callback(folder);
                    } catch (err) {
                        console.error('Error in log change callback:', err);
                    }
                });
            } else {
                console.error(`Failed to change runtime template to "${folder}"`);
            }
        } catch (error) {
            console.error('Template change error:', error);
        } finally {
            this._releaseLock();
            this.selector.disabled = false;
        }
    }

    _acquireLock() {
        localStorage.setItem(this.lockKey, Date.now().toString());
    }

    _releaseLock() {
        localStorage.removeItem(this.lockKey);
    }
}
