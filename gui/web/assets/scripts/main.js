/* global Stimulus, Viz */
/* exported main */

// main is the entry point of the script, called when the DOM is loaded
//
// This script relies on the stimulus library to bind data between the HTML and
// the javascript. In short each HTML element with a special
// `data-XXX-target=yyy` attribute can be accessed in the corresponding `XXX`
// controller as the `this.yyyTarget` variable. See
// https://stimulus.hotwired.dev/ for a complete introduction.

const main = function () {
    const application = Stimulus.Application.start();

    application.register("flash", Flash);
    application.register("peerInfo", PeerInfo);
    application.register("messaging", Messaging);
    application.register("unicast", Unicast);
    application.register("routing", Routing);
    application.register("packets", Packets);
    application.register("broadcast", Broadcast);

    initCollapsible();
};

function initCollapsible() {
    // https://www.w3schools.com/howto/howto_js_collapsible.asp
    var coll = document.getElementsByClassName("collapsible");
    for (var i = 0; i < coll.length; i++) {
        coll[i].addEventListener("click", function () {
            var content = this.nextElementSibling;
            if (this.classList.contains("active")) {
                content.style.display = "none";
                this.classList.remove("active");
            } else {
                content.style.display = "block";
                this.classList.add("active");
            }
        });

        var content = coll[i].nextElementSibling;
        if (coll[i].classList.contains("active")) {
            content.style.display = "block";
        } else {
            content.style.display = "none";
        }
    }
}

// BaseElement is inherited by all the controllers. It provides common methods.
class BaseElement extends Stimulus.Controller {

    // checkInputs is a utility function to checks if form inputs are empty or
    // not. It takes care of displaying an appropriate message if a validation
    // fails. The caller should exit if this function returns false.
    checkInputs(...els) {
        for (let i = 0; i < els.length; i++) {
            const val = els[i].value;
            if (val == "" || val == undefined) {
                this.flash.printError(`form validation failed: "${els[i].name}" is empty or invalid`);
                return false;
            }
        }

        return true;
    }

    // fetch fetches the addr and checks for a 200 status in return.
    async fetch(addr, opts) {
        const resp = await fetch(addr, opts);
        if (resp.status != 200) {
            const text = await resp.text();
            throw `wrong status: ${resp.status} ${resp.statusText} - ${text}`;
        }

        return resp;
    }

    get flash() {
        const element = document.getElementById("flash");
        return this.application.getControllerForElementAndIdentifier(element, "flash");
    }

    get peerInfo() {
        const element = document.getElementById("peerInfo");
        return this.application.getControllerForElementAndIdentifier(element, "peerInfo");
    }
}

class Flash extends Stimulus.Controller {
    static get targets() {
        return ["wrapper"];
    }

    printError(e) {
        const flash = this.createFlash("error", e);
        this.wrapperTarget.appendChild(flash);
    }

    printSuccess(e) {
        const flash = this.createFlash("success", e);
        this.wrapperTarget.appendChild(flash);
    }

    createFlash(className, content) {
        content = content.replace(/\s+/g, ' ').trim();

        const newDiv = document.createElement("div");
        newDiv.classList.add("flash-msg", className);

        const closeBtn = document.createElement("span");

        closeBtn.innerHTML = "&times;";
        closeBtn.classList.add("closebtn");
        closeBtn.addEventListener("click", (e) => {
            e.target.parentElement.remove();
        });

        const progress = document.createElement("span");
        progress.classList.add("progress");

        newDiv.innerText = content;
        newDiv.appendChild(closeBtn);
        newDiv.appendChild(progress);

        setTimeout(() => {
            newDiv.remove();
        }, 4000);

        return newDiv;
    }
}

class PeerInfo extends Stimulus.Controller {
    static get targets() {
        return ["peerAddr", "socketAddr"];
    }

    async initialize() {
        const queryDict = this.getQueryArgs();

        const endpoint = "http://" + decodeURIComponent(queryDict["addr"]);
        this.endpoint = endpoint;

        this.peerAddrTarget.innerText = this.endpoint;
        console.log(this.endpoint);

        const addr = this.endpoint + "/socket/address";

        try {
            const resp = await fetch(addr);
            const text = await resp.text();

            this.socketAddrTarget.innerText = "tcp://" + text;
            this.socketAddr = text;
        } catch (e) {
            this.flash.printError("failed to fetch socket address: " + e);
        }
    }

    getAPIURL(suffix) {
        return this.endpoint + suffix;
    }

    get flash() {
        const element = document.getElementById("flash");
        return this.application.getControllerForElementAndIdentifier(element, "flash");
    }

    // getQueryArgs returns a dictionary of key=value arguments from the URL
    getQueryArgs() {
        const queryDict = {};
        location.search.substr(1).split("&").forEach(function (item) {
            queryDict[item.split("=")[0]] = item.split("=")[1];
        });

        return queryDict;
    }
}

class Messaging extends BaseElement {
    static get targets() {
        return ["holder", "messages"];
    }

    addMsg(el) {
        this.messagesTarget.append(el);
        this.holderTarget.scrollTop = this.holderTarget.scrollHeight;
    }
}

class Unicast extends BaseElement {
    static get targets() {
        return ["message", "destination"];
    }

    async send() {
        const addr = this.peerInfo.getAPIURL("/messaging/unicast");

        const ok = this.checkInputs(this.messageTarget, this.destinationTarget);
        if (!ok) {
            return;
        }

        const message = this.messageTarget.value;
        const destination = this.destinationTarget.value;

        const pkt = {
            "Dest": destination,
            "Msg": {
                "Type": "chat",
                "payload": {
                    "Message": message
                }
            }
        };

        const fetchArgs = {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(pkt)
        };

        try {
            await this.fetch(addr, fetchArgs);

            const date = new Date();
            const el = document.createElement("div");

            el.classList.add("sent");
            el.innerHTML = `
                <div>
                    <p class="msg">${message}</p>
                    <p class="details">
                        to ${destination} 
                        at ${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}
                    </p>
                </div>`;

            this.messagingController.addMsg(el);
        } catch (e) {
            this.flash.printError("failed to send message: " + e);
        }
    }

    get messagingController() {
        const element = document.getElementById("messaging");
        return this.application.getControllerForElementAndIdentifier(element, "messaging");
    }
}


class Routing extends BaseElement {
    static get targets() {
        return ["table", "graphviz", "peer", "origin", "relay"];
    }

    initialize() {
        this.update();
    }

    async update() {
        const addr = this.peerInfo.getAPIURL("/messaging/routing");

        try {
            const resp = await this.fetch(addr);
            const data = await resp.json();

            this.tableTarget.innerHTML = "";

            for (const [origin, relay] of Object.entries(data)) {
                const el = document.createElement("tr");

                el.innerHTML = `<td>${origin}</td><td>${relay}</td>`;
                this.tableTarget.appendChild(el);
            }

            this.flash.printSuccess("Routing table updated");

        } catch (e) {
            this.flash.printError("Failed to fetch routing: " + e);
        }

        const graphAddr = addr + "?graphviz=on";

        try {
            const resp = await this.fetch(graphAddr);
            const data = await resp.text();

            var viz = new Viz();

            const element = await viz.renderSVGElement(data);
            this.graphvizTarget.innerHTML = "";
            this.graphvizTarget.appendChild(element);
        } catch (e) {
            this.flash.printError("Failed to display routing: " + e);
        }
    }

    async addPeer() {
        const ok = this.checkInputs(this.peerTarget);
        if (!ok) {
            return;
        }

        const addr = this.peerInfo.getAPIURL("/messaging/peers");
        const peer = this.peerTarget.value;

        const fetchArgs = {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify([peer])
        };

        try {
            await this.fetch(addr, fetchArgs);
            this.flash.printSuccess("peer added");
            this.update();
        } catch (e) {
            this.flash.printError("failed to add peer: " + e);
        }
    }

    async setEntry() {
        const addr = this.peerInfo.getAPIURL("/messaging/routing");

        const ok = this.checkInputs(this.originTarget);
        if (!ok) {
            return;
        }

        const origin = this.originTarget.value;
        const relay = this.relayTarget.value;

        const message = {
            "Origin": origin,
            "RelayAddr": relay
        };

        const fetchArgs = {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(message)
        };

        try {
            await this.fetch(addr, fetchArgs);

            if (relay == "") {
                this.flash.printSuccess("Entry deleted");
            } else {
                this.flash.printSuccess("Entry set");
            }

            this.update();

        } catch (e) {
            this.flash.printError("failed to set entry: " + e);
        }
    }
}

class Packets extends BaseElement {
    static get targets() {
        return ["follow", "holder", "scroll", "packets"];
    }

    initialize() {
        const addr = this.peerInfo.getAPIURL("/registry/pktnotify");
        const newPackets = new EventSource(addr);

        newPackets.onmessage = this.packetMessage.bind(this);
        newPackets.onerror = this.packetError.bind(this);

        this.holderTarget.addEventListener("scroll", this.packetsScroll.bind(this));
    }

    packetMessage(e) {
        const pkt = JSON.parse(e.data);
        if (pkt.Msg.Type == "chat") {
            const date = new Date(pkt.Header.Timestamp / 1000000);

            const el = document.createElement("div");

            if (pkt.Header.Source == this.peerInfo.socketAddr) {
                el.classList.add("sent");
            } else {
                el.classList.add("received");
            }

            // note that this is not secure and prone to XSS attack.
            el.innerHTML = `<div><p class="msg">${pkt.Msg.Payload.Message}</p><p class="details">from ${pkt.Header.Source} at ${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}</p></div>`;

            this.messagingController.addMsg(el);
        }

        const el = document.createElement("div");
        el.innerHTML = `<pre>${JSON.stringify(pkt, null, 2)}</pre>`;

        this.packetsTarget.append(el);

        this.scrollTarget.style.width = this.packetsTarget.scrollWidth + "px";

        if (this.followTarget.checked) {
            this.packetsTarget.scrollLeft = this.packetsTarget.scrollWidth;
            this.holderTarget.scrollLeft = this.holderTarget.scrollWidth;
        }
    }

    packetError() {
        this.flash.printError("failed to listen pkt: stopped listening");
    }

    packetsScroll() {
        this.packetsTarget.scrollLeft = this.holderTarget.scrollLeft;
    }

    get messagingController() {
        return this.application.getControllerForElementAndIdentifier(document.getElementById("messaging"), "messaging");
    }
}

class Broadcast extends BaseElement {
    static get targets() {
        return ["chatMessage", "privateMessage", "privateRecipients"];
    }

    async sendChat() {
        const addr = this.peerInfo.getAPIURL("/messaging/broadcast");

        const ok = this.checkInputs(this.chatMessageTarget);
        if (!ok) {
            return;
        }

        const message = this.chatMessageTarget.value;

        const msg = {
            "Type": "chat",
            "payload": {
                "Message": message
            }
        };

        const fetchArgs = {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(msg)
        };

        try {
            await this.fetch(addr, fetchArgs);
            this.flash.printSuccess("chat message broadcasted");
        } catch (e) {
            this.flash.printError("failed to send message: " + e);
        }
    }

    async sendPrivate() {
        const addr = this.peerInfo.getAPIURL("/messaging/broadcast");

        const ok = this.checkInputs(this.privateMessageTarget, this.privateRecipientsTarget);
        if (!ok) {
            return;
        }

        const destination = this.privateRecipientsTarget.value;
        const message = this.privateMessageTarget.value;

        const recipients = {};
        destination.split(",").forEach(e => recipients[e.trim()] = {});

        const msg = {
            "Type": "private",
            "payload": {
                "Recipients": recipients,
                "Msg": {
                    "Type": "chat",
                    "payload": {
                        "Message": message
                    }
                }
            }
        };

        const fetchArgs = {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(msg)
        };

        try {
            await this.fetch(addr, fetchArgs);
            this.flash.printSuccess("private message sent");
        } catch (e) {
            this.flash.printError("failed to send message: " + e);
        }
    }
}

