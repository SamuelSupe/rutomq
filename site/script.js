const packageOptions = {
  deb: {
    x86_64: {
      filename: "rutomq_0.1.0_amd64.deb",
      install: "sudo apt install ./rutomq_0.1.0_amd64.deb",
    },
    arm64: {
      filename: "rutomq_0.1.0_arm64.deb",
      install: "sudo apt install ./rutomq_0.1.0_arm64.deb",
    },
  },
  rpm: {
    x86_64: {
      filename: "rutomq-0.1.0-1.x86_64.rpm",
      install: "sudo dnf install ./rutomq-0.1.0-1.x86_64.rpm",
    },
    arm64: {
      filename: "rutomq-0.1.0-1.aarch64.rpm",
      install: "sudo dnf install ./rutomq-0.1.0-1.aarch64.rpm",
    },
  },
};

const state = {
  package: "deb",
  arch: "x86_64",
};

const commandNode = document.querySelector("[data-install-command]");
const filenameNode = document.querySelector("[data-package-filename]");
const copyButton = document.querySelector("[data-copy-command]");
const copyStatus = document.querySelector("[data-copy-status]");

function commandForSelection() {
  const selected = packageOptions[state.package][state.arch];
  return [
    `curl -LO https://github.com/SamuelSupe/rutomq/releases/download/v0.1.0/${selected.filename}`,
    selected.install,
    "sudoedit /etc/rutomq/rutomq.env",
    "sudo systemctl enable --now rutomq",
  ].join("\n");
}

function renderPackageSelection() {
  const selected = packageOptions[state.package][state.arch];
  commandNode.textContent = commandForSelection();
  filenameNode.textContent = selected.filename;
  copyStatus.textContent = "";

  document.querySelectorAll("[data-package]").forEach((button) => {
    button.setAttribute("aria-pressed", String(button.dataset.package === state.package));
  });
  document.querySelectorAll("[data-arch]").forEach((button) => {
    button.setAttribute("aria-pressed", String(button.dataset.arch === state.arch));
  });
}

document.querySelectorAll("[data-package]").forEach((button) => {
  button.addEventListener("click", () => {
    state.package = button.dataset.package;
    renderPackageSelection();
  });
});

document.querySelectorAll("[data-arch]").forEach((button) => {
  button.addEventListener("click", () => {
    state.arch = button.dataset.arch;
    renderPackageSelection();
  });
});

copyButton.addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(commandForSelection());
    copyStatus.textContent = "Install commands copied.";
    copyButton.textContent = "Copied";
    window.setTimeout(() => {
      copyButton.textContent = "Copy";
    }, 1800);
  } catch {
    copyStatus.textContent = "Copy was blocked. Select the commands manually.";
  }
});

const navToggle = document.querySelector("[data-nav-toggle]");
const navLinks = document.querySelector("[data-nav-links]");

navToggle.addEventListener("click", () => {
  const isOpen = navLinks.classList.toggle("is-open");
  navToggle.setAttribute("aria-expanded", String(isOpen));
});

navLinks.querySelectorAll("a").forEach((link) => {
  link.addEventListener("click", () => {
    navLinks.classList.remove("is-open");
    navToggle.setAttribute("aria-expanded", "false");
  });
});

const revealObserver = new IntersectionObserver(
  (entries, observer) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting) {
        entry.target.classList.add("is-visible");
        observer.unobserve(entry.target);
      }
    });
  },
  { threshold: 0.14 },
);

document.querySelectorAll(".reveal").forEach((element) => {
  revealObserver.observe(element);
});

renderPackageSelection();
