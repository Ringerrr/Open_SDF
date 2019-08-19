create table gui_overlays
(
    ID            integer       primary key
  , OverlayName   text
  , OverlayPath   text
  , Active        integer       default 1
);
