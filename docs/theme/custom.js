document.addEventListener("DOMContentLoaded", () => {
  const title = document.querySelector(".menu-title");
  if (title && title.dataset.hpcComposeLogo !== "true") {
    const logo = document.createElement("img");
    logo.src = `${window.HPC_COMPOSE_ROOT || ""}favicon.png`;
    logo.alt = "";
    logo.setAttribute("aria-hidden", "true");
    logo.className = "hpc-compose-header-logo";

    title.prepend(logo);
    title.dataset.hpcComposeLogo = "true";
  }

  const scrollableTables = [];
  document.querySelectorAll("main table").forEach((table) => {
    if (table.parentElement?.classList.contains("table-scroll")) {
      return;
    }

    const wrapper = document.createElement("div");
    wrapper.className = "table-scroll";
    const columnCount = Math.max(
      0,
      ...Array.from(table.rows).map((row) => row.cells.length),
    );
    if (columnCount >= 3) {
      wrapper.classList.add("wide");
    }
    table.parentNode.insertBefore(wrapper, table);
    wrapper.appendChild(table);
    scrollableTables.push(wrapper);
  });

  const updateTableFocus = () => {
    scrollableTables.forEach((wrapper) => {
      const overflows = wrapper.scrollWidth > wrapper.clientWidth + 1;
      wrapper.tabIndex = overflows ? 0 : -1;
      if (overflows) {
        wrapper.setAttribute("aria-label", "Scrollable table");
      } else {
        wrapper.removeAttribute("aria-label");
      }
    });
  };
  window.requestAnimationFrame(updateTableFocus);
  window.addEventListener("resize", updateTableFocus);

  document.querySelectorAll('main input[type="checkbox"]').forEach((checkbox) => {
    const itemText = checkbox.parentElement?.textContent?.trim();
    checkbox.setAttribute(
      "aria-label",
      itemText ? `Checklist item: ${itemText}` : "Checklist item",
    );
  });
});
