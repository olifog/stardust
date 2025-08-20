declare module "three-spritetext" {
  export default class SpriteText {
    constructor(text?: string);
    text: string;
    textHeight: number;
    color: string;
    center: { x: number; y: number };
  }
}


