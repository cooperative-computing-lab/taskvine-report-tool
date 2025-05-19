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

    _createInput(type = "number") {
        if (this._type !== "input") {
            return;
        }

        this._input = document.createElement("input");
        this._input.placeholder = "Input";
        this._input.type = type;
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

    unbindGlobalListeners() {
        if (this._boundDocClick) {
            document.removeEventListener("click", this._boundDocClick);
        }
        if (this._boundEsc) {
            document.removeEventListener("keydown", this._boundEsc);
        }
    }

    _renderPanel() {
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

    _renderToggle() {
        this.toggle = document.createElement("button");
        this.toggle.textContent = this.title;
        this.toggle.className = "toolbox-toggle-button";
    
        this.toggle.addEventListener("click", (e) => {
            e.stopPropagation();
            this._showPanel();
        });
    
        this._boundDocClick = (e) => {
            if (!this.panel.contains(e.target) && !this.toggle.contains(e.target)) {
                this._hidePanel();
            }
        };
        document.addEventListener("click", this._boundDocClick);
    
        this._setupKeyboardShortcuts();
    }
    
    _setupKeyboardShortcuts() {
        this._boundEsc = (e) => {
            if (e.key === "Escape" && this.panel && this.panel.classList.contains("show")) {
                this._hidePanel();
            }
        };
        document.addEventListener("keydown", this._boundEsc);
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

    createButtonItem(id, label, handler) {
        return {
            id,
            buttonLabel: label,
            type: "button",
            handler,
        };
    }

    createInputItem(id, label, handler) {
        return {
            id,
            buttonLabel: label,
            type: "input",
            handler,
        };
    }

    createSelectorItem(id, label, options, handler) {
        return {
            id,
            buttonLabel: label,
            type: "selector",
            selectorOptions: options,
            handler,
        };
    } 

    setItems(items) {
        this.items = items.map((item) => new ToolboxItem(item));
    }

    mount(container) {
        const wrapper = document.createElement("div");
        wrapper.className = "toolbox-wrapper";
        wrapper.style.position = "relative";
        wrapper.style.width = "100%";
        wrapper.style.height = "100%";

        this._renderPanel();
        this._renderToggle();

        wrapper.appendChild(this.toggle);
        wrapper.appendChild(this.panel);
        container.appendChild(wrapper);
    }

    destroy() {
        this.unbindGlobalListeners();
    
        if (this.panel?.parentNode) {
            this.panel.remove();
            this.panel = null;
        }
        if (this.toggle?.parentNode) {
            this.toggle.remove();
            this.toggle = null;
        }
    
        this.items = [];
    }    
}
