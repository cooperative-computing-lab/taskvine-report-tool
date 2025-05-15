// toolbox.js
export class ToolboxItem {
    constructor({ buttonLabel, id, type = "button", selectorOptions = [], handler = null }) {
        if (type !== "button" && type !== "input" && type !== "selector") {
            throw new Error("Invalid type");
        }

        this._buttonLabel = buttonLabel;
        this._id = id;
        this._type = type;
        this._selectorOptions = selectorOptions;
        this._handler = handler;
    }

    render() {
        this._createInput();
        this._createSelector();
        this._createButton();
        this._bindButtonHandler();

        return this._createToolboxRow();
    }

    _createToolboxRow() {
        const row = document.createElement("div");
        switch (this._type) {
            case "input":
                row.className = "toolbox-row-split";
                row.appendChild(this._input);
                break;
            case "selector":
                row.className = "toolbox-row-split";
                row.appendChild(this._selector);
                break;
            case "button":
                row.className = "toolbox-row-full";
                break;
        }
        row.appendChild(this._button);
        return row;
    }

    _createInput() {
        if (this._type !== "input") {
            return;
        }

        this._input = document.createElement("input");
        this._input.placeholder = "Input";
        this._input.className = "toolbox-input";
        this._input.id = `input-${this._id}`;
    }

    _createSelector() {
        if (this._type !== "selector") {
            return;
        }

        this._selector = document.createElement("select");
        this._selector.className = "toolbox-selector";
        this._selector.id = `selector-${this._id}`;

        this._selectorOptions.forEach((option) => {
            const optionElement = document.createElement("option");
            optionElement.value = option.value;
            optionElement.textContent = option.label;
            this._selector.appendChild(optionElement);
        });
    }

    _createButton() {
        this._button = document.createElement("button");
        this._button.textContent = this._buttonLabel;
        this._button.className = "report-button";
    }

    _bindButtonHandler() {
        if (this._type === "button") {
            this._button.addEventListener("click", () => this._handler(this._id));
        } else if (this._type === "selector") {
            this._button.addEventListener("click", () => {
                this._handler(this._id, this._selector.value);
            });
        } else if (this._type === "input") {
            this._button.addEventListener("click", () => {
                this._handler(this._id, this._input.value);
            });
        }
    }
}

export class Toolbox {
    constructor({ id, title = 'Toolbox', items = [] }) {
        this.id = id;
        this.title = title;
        this.items = items.map((item) => new ToolboxItem(item));
        this.panel = null;
        this.toggle = null;
    }

    renderPanel() {
        this.panel = document.createElement("div");
        this.panel.className = "toolbox-panel hide";
        this.panel.id = this.id;

        const group = document.createElement("div");
        group.className = "toolbox-group";

        this.items.forEach((item) => {  
            group.appendChild(item.render());
        });

        this.panel.appendChild(group);
    }

    renderToggle() {
        this.toggle = document.createElement("button");
        this.toggle.textContent = this.title;
        this.toggle.className = "toolbox-toggle-button";
        this.toggle.addEventListener("click", (e) => {
            e.stopPropagation();
            this._showPanel();
        });

        document.addEventListener("click", (e) => {
            if (!this.panel.contains(e.target) && !this.toggle.contains(e.target)) {
                this._hidePanel();
            }
        });

        this._setupKeyboardShortcuts();
    }

    _showPanel() {
        this.panel.classList.add("show");
        this.panel.classList.remove("hide");
    }

    _hidePanel() {
        this.panel.classList.add("hide");
        this.panel.classList.remove("show");
        const inputs = this.panel.querySelectorAll(".toolbox-input");
        inputs.forEach((input) => {
            input.value = "";
        });
    }

    _setupKeyboardShortcuts() {
        document.addEventListener("keydown", (e) => {
            if (e.key === "Escape") {
                if (this.panel && this.panel.classList.contains("show")) {
                    this._hidePanel();
                }
            }
        });
    }

    mount(container) {
        const wrapper = document.createElement("div");
        wrapper.className = "toolbox-wrapper";
        wrapper.style.position = "relative";
        wrapper.style.width = "100%";
        wrapper.style.height = "100%";

        this.renderPanel();
        this.renderToggle();

        wrapper.appendChild(this.toggle);
        wrapper.appendChild(this.panel);
        container.appendChild(wrapper);
    }
}



export function createToolbox(id) {
    const config = {
        id: `${id}-toolbox`,
        title: 'Toolbox',
        items: [
            {
                buttonLabel: 'Update X', 
                id: 'updateX',
                type: 'input',
                handler: (id, value) => console.log(`${id} => ${value}`)
            },
            { 
                buttonLabel: 'Refresh', 
                id: 'refresh', 
                type: 'button',
                handler: (id) => console.log(`${id} clicked`)
            },
            {
                buttonLabel: 'Extract', 
                id: 'extract', 
                type: 'selector',
                handler: (id, value) => console.log(`${id} => ${value}`),
                selectorOptions: [
                    { 
                        value: 'mode1', 
                        label: 'Mode 1',
                    },
                    { 
                        value: 'mode2', 
                        label: 'Mode 2',
                    },
                    { 
                        value: 'mode3', 
                        label: 'Mode 3',
                    }
                ]
            }
        ]
    }

    return new Toolbox(config);
}