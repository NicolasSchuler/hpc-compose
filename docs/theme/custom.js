document.addEventListener("DOMContentLoaded", () => {
  const title = document.querySelector(".menu-title");
  if (!title || title.dataset.hpcComposeLogo === "true") {
    return;
  }

  const logo = document.createElement("img");
  logo.src = `${window.HPC_COMPOSE_ROOT || ""}logo.png`;
  logo.alt = "";
  logo.setAttribute("aria-hidden", "true");
  logo.className = "hpc-compose-header-logo";

  title.prepend(logo);
  title.dataset.hpcComposeLogo = "true";
});
